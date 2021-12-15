import time, datetime,os,argparse
import json,sys,socket, re, globals
import syslog, pdb
import pandas as pd
import numpy as np
import math
import operative_functions as asf
from spade.agent import Agent
from spade.behaviour import PeriodicBehaviour
from spade.template import Template

from spade import quit_spade

class CoilAgent(Agent):
    class CoilBehav(PeriodicBehaviour):
        async def run(self):
            global my_full_name, my_dir, wait_msg_time, coil_status_var, \
                    coil_started_at,stop_time, refresh_time, coil_agent, \
                    coil_df, bid_register_df, number_auction, seq_coil,\
                    auction_finish_at, ip_machine, va_coil_msg_df
            #
            if coil_status_var == "auction":
                """inform log of status"""
                msg = await self.receive(timeout=auction_time)
                if msg:
                    # print(' reading: ' + msg.body)
                    seq_coil = seq_coil + 1
                    msg_sender_jid = str(msg.sender).split('/')[0].upper()
                    sender_jid     = msg_sender_jid.upper().split('@')[0]
                    tgt = globals.pth[globals.ipth]
                    res = re.match(tgt,sender_jid)
                    # Message targeted for VA fitting into the right plant.
                    if "VA" in sender_jid and res != None: # res => VA is the next
                        """Inform log """
                        msgbdy         = pd.read_json(msg.body)
                        va_coil_msg_df = pd.read_json(msgbdy.loc[0,'msg'])
                        #
                        # Inform log
                        req = 'Answering VA request'
                        seqc= 'Request from ' + msg_sender_jid + msgbdy.loc[0,'purpose']
                        coil_msg_log= asf.log_req_va (my_full_name,req, seqc, \
                                        msg_sender_jid)
                        cl_msg_json = coil_msg_log.to_json(orient="records")
                        cl_msg_log  = asf.msg_to_log(cl_msg_json, my_dir)
                        await self.send(cl_msg_log)
                        #
                        coil_enter= 0  # Is the location of the coil compatible with the plant ?
                        z = re.match(path[0],sender_jid)
                        if z != None:  # The plant is a target otherwise not.
                            coil_enter = 1
                        #
                        if bid_register_df.shape[0] > 0: # If the coil already passed
                            p0j = bid_register_df['agent_type'] == 'VA'
                            p1j = bid_register_df['status'] == 'won'
                            if bid_register_df[p0j & p1j].shape[0] > 0:
                                p1j = bid_register_df['status'] == ''
                                bid_register_df.loc[p0j & p1j,'status'] = 'gone'
                        if msgbdy.loc[0,'purpose'] == 'invitation':
                            if bid_register_df.shape[0] > 0:
                                p0j = bid_register_df['agent_type'] == 'VA'
                                p1j = bid_register_df['status'] == 'won'
                                if bid_register_df[p0j & p1j].shape[0] > 0:
                                    coil_enter = 0 # Already passed
                            if coil_enter == 1: # Bid suitable for offer
                                # Create bid
                                bid_mean = va_coil_msg_df.loc[0, 'bid_mean']
                                bid = asf.create_bid(coil_df, bid_mean)
                                coil_df.loc[0,'bid'] = bid
                                reg_bid = pd.DataFrame([{'id': my_full_name,\
                                        'Coil':my_full_name, \
                                        'idres':msg_sender_jid, 'agent_type':'VA',\
                                        'auction_dt':datetime.datetime.now(),\
                                        'decision_dt': '',\
                                        'initial_bid':bid,'second_bid':-1,\
                                        'accepted_bid':-1,'won_bid':-1,\
                                        'status':''}])
                                if bid_register_df.shape[0] > 0:
                                    bidx = bid_register_df.index[bid_register_df[\
                                            'idres']==msg_sender_jid]
                                    for ibid in bidx:
                                        if bid_register_df.loc[ibid,'status'] == '':
                                            bid_register_df.loc[ibid,'status'] = 'gone'
                                bid_register_df = pd.concat([bid_register_df, reg_bid],\
                                        ignore_index=True)
                                bid_register_df.set_index('id',drop=False)
                                if coil_df.loc[0,'init_va_auction'] is None:
                                    coil_df.loc[0,'init_va_auction'] = \
                                        datetime.datetime.now()
                                """ Inform log """
                                coil_msg_log_body = asf.send_to_va_msg(\
                                        my_full_name, bid, globals.glog_jid, '1')
                                cl_msg_log = coil_msg_log_body.to_json(\
                                        orient="records")
                                cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                                await self.send(cl_log)
                            else:  # Not interested in such RFQ
                                """ Inform log """
                                coil_msg_log_body = asf.send_to_va_msg(\
                                        my_full_name, 'Not match: Not offering',\
                                        globals.glog_jid, '1')
                                cl_msg_log = coil_msg_log_body.to_json(\
                                        orient="records")
                                cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                                await self.send(cl_log)
                                coil_df.loc[0,'bid'] = 0
                            #
                            coil_df.loc[0,'budget_remaining'] = coil_df.loc[0,\
                                    'budget'] - coil_df.loc[0,'bid']
                            """ Send answer to VA agent """
                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                        msg_sender_jid,'answer2invitation',\
                                        msgbdy.loc[0,'seq'])
                            cl_ans  = asf.contact_list_json(cl_agent,\
                                        msg_sender_jid)
                            await self.send(cl_ans)
                            coil_df.loc[0, 'number_auction'] = coil_df.loc[\
                                        0, 'number_auction'] + 1
                        #
                        if msgbdy.loc[0,'purpose'] == 'counterbid':
                            """ Receive request to counterbid """
                            counterbid = asf.create_counterbid(\
                                        va_coil_msg_df,coil_df)
                            if bid_register_df.shape[0] > 0:
                                p0j = bid_register_df['agent_type'] == 'VA'
                                p1j = bid_register_df['status'] == 'won'
                                if bid_register_df[p0j & p1j].shape[0] > 0:
                                    counterbid = 0 # Already passed
                            """ Prepare bid to send to va """
                            coil_df.loc[0, 'counterbid'] = counterbid
                            coil_df['User_name_va'] = str(msg_sender_jid)
                            coil_df['budget_remaining'] = coil_df.loc[0, \
                                    'budget'] - coil_df.loc[0, 'counterbid']
                            p0j = bid_register_df.idres == msg_sender_jid
                            p1j = bid_register_df.second_bid == -1
                            pj  = bid_register_df.index[p0j & p1j]
                            bid_register_df.loc[pj,'second_bid'] = counterbid
                            """ Answering to the VA """
                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                        msg_sender_jid,'answer2counterbid',\
                                                  msgbdy.loc[0,'seq'])
                            cl_ans  = asf.contact_list_json(cl_agent,\
                                        msg_sender_jid)
                            await self.send(cl_ans)
                            #
                            """ Inform log """
                            msg_lbdy = asf.send_to_va_msg(\
                                    my_full_name,counterbid,globals.glog_jid,'1')
                            cl_msg_log = msg_lbdy.to_json(orient="records")
                            cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                            await self.send(cl_log)
                            # print(' Answeing:'+msg.body)
                            # print(cl_agent)
                            # print('***')
                        #
                        if msgbdy.loc[0,'purpose'] == 'notprofit':
                            """ Receive request to counterbid """
                            bid_register_df.loc[max(bid_register_df.index),\
                                            'status'] = 'lost'
                            bid_register_df.loc[max(bid_register_df.index),\
                                            'decision_dt'] = datetime.datetime.now()
                            coil_status_var = 'stand-by'
                        #
                        if msgbdy.loc[0,'purpose'] == 'confirm':
                            """Receive request to confirm as winner """
                            """ Answering to the VA """
                            # print(' Answeing Confirm:'+msg.body)
                            va_coil_msg_df = pd.read_json(msgbdy.loc[0,'msg'])
                            # print(va_coil_msg_df)
                            # print('===')
                            #
                            if va_coil_msg_df.at[0, 'bid_status'] == 'acceptedbid':
                                # Store accepted Bid from ca agent
                                bid_register_df = asf.update_bid_va(bid_register_df,va_coil_msg_df)
                                #
                                p0j = bid_register_df.idres== msg_sender_jid
                                p1j = bid_register_df.accepted_bid == -1
                                pj  = bid_register_df.index[p0j & p1j]
                                ppj = -1
                                for ipj in pj:
                                    if bid_register_df.loc[ipj,'status'] == '':
                                        ppj = ipj
                                if ppj > -1:
                                    bid_register_df.loc[ppj,'accepted_bid'] = \
                                            bid_register_df.loc[ppj,'second_bid']
                                    bid_register_df.loc[ppj,'seq']= msgbdy.loc[0,'seq']
                                else:
                                    aid  = va_coil_msg_df.at[0, 'id']
                                    astr = f'ID: {aid}. Not opened bids in the register:'
                                    astr = astr + f' Coil: {my_full_name}'
                                    coil_msg_log_body = asf.inform_log(my_full_name,\
                                                        astr,globals.glog_jid)
                                    cl_msg_lg_bd = coil_msg_log_body.to_json(orient="records")
                                    coil_msg_log = asf.msg_to_log(cl_msg_lg_bd, my_dir)
                                    await self.send(coil_msg_log)
                                #
                                # Pending bids from different VA plants ???
                                p0j = bid_register_df.agent_type == 'VA'
                                p2j = bid_register_df.status == ''
                                p3j = bid_register_df.idres != msg_sender_jid
                                pj  = bid_register_df.index[p0j & p1j & p2j & p3j]
                                if sum(pj) == 0: # We can decide as no additional
                                                 # invitations are pending
                                    pj = bid_register_df.index[p0j & p2j]
                                    minacc=bid_register_df.loc[pj,'accepted_bid'].min()
                                    p3j= bid_register_df.accepted_bid==minacc
                                    p1j= bid_register_df['status'] == 'won'
                                    pjs= bid_register_df.index[p0j & p2j & p3j]
                                    if bid_register_df[p0j & p1j].shape[0] > 0:
                                        p1j = bid_register_df['status'] == ''
                                        bid_register_df.loc[p0j & p1j & p3j,'status'] = 'gone'
                                        pjs= []
                                    for icl in pj:
                                        plant_id = bid_register_df.loc[icl,'idres']
                                        sqf = bid_register_df.loc[icl,'seq']
                                        if icl not in pjs: # Offers not winning
                                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                                            plant_id,'NOTacceptedbid',sqf)
                                            cl_ans  = asf.contact_list_json(cl_agent,\
                                                            plant_id)
                                            await self.send(cl_ans)
                                            bid_register_df.loc[icl,'status'] = 'lost'
                                        else:
                                            coil_df.loc[0, 'bid_status'] = 'acceptedbid'
                                            auction_finish_at = datetime.datetime.now()
                                            cl_msg_lbdy = asf.won_auction(my_full_name, \
                                                    sqf, auction_finish_at)
                                            coil_msg_js = asf.msg_to_log(cl_msg_lbdy, my_dir)
                                            await self.send(coil_msg_js)
                                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                                            plant_id,'OKacceptedbid',sqf)
                                            cl_ans  = asf.contact_list_json(cl_agent,\
                                                            plant_id)
                                            await self.send(cl_ans)
                                            bid_register_df.loc[icl,'status'] = 'won'
                                            # Changing the position of the coil and ending.
                                            globals.ipth = globals.ipth + 1
                                            if 'END' not in globals.pth:
                                                globals.pth.append('END')
                                            tgt = globals.pth[globals.ipth]
                                            if tgt == 'END': # Coil Ended
                                                cbid = coil_df.loc[0,'counterbid']
                                                dt_end = datetime.datetime.now().strftime(\
                                                            "%Y-%m-%d %H:%M:%S")
                                                msg_str = f'ENDEDUP: {my_full_name}, code:{code} '
                                                msg_str = msg_str+f', order: {ordr}, by: {dt_end}'
                                                msg_str = msg_str+f', offer: {cbid}'
                                                cl_msg_lbdy = asf.inform_log(my_full_name,\
                                                            msg_str,globals.glog_jid)
                                                cl_msg_lg_bd= cl_msg_lbdy.to_json(orient="records")
                                                coil_msg_log= asf.msg_to_log(cl_msg_lg_bd, my_dir)
                                                await self.send(coil_msg_log)
                                                self.kill()
                            #
                            if va_coil_msg_df.at[0, 'bid_status'] == 'rejectedbid':
                                pj = bid_register_df['idres']==msg_sender_jid and \
                                        bid_register_df['accepted_bid'] == -1 and \
                                        bid_register_df['status'] == ''
                                bid_register_df.loc[pj,'accepted_bid'] =  -10
                                bid_register_df.loc[pj,'won_bid'] =  -10
                        #
                        if msgbdy.loc[0,'purpose'] not in ['invitation','counterbid',\
                                                'confirm','notprofit']:
                            """inform log of status"""
                            # print(' ELSE:'+ msgbdy.loc[0,'purpose'])
                            number_auction += int(1)
                            number_astr = f'{my_full_name} did not enter {msg_sender_jid} auction '
                            number_astr = number_astr + 'because does not meet the '
                            number_astr = number_astr + 'requirements. Not_entered auction '
                            number_astr = number_astr + f'number: {number_auction}. Purpose:'
                            number_astr = number_astr + msgbdy.loc[0,'purpose']
                            coil_msg_log_body = asf.inform_log(my_full_name,\
                                                number_astr,globals.glog_jid)
                            cl_msg_lg_bd = coil_msg_log_body.to_json(orient="records")
                            coil_msg_log = asf.msg_to_log(cl_msg_lg_bd, my_dir)
                            await self.send(coil_msg_log)
                            coil_status_var = 'stand-by'
                        del msg
                    elif "LA" in msg_sender_jid:
                        # Message from Launcher requesting parameter update ...
                        msgl = pd.read_json(msg.body)
                        msg_sender_jid = (msg_sender_jid.split('@')[0])
                        if msgl.loc[0,'purpose'] == 'update':
                            # Parameter adjustment
                            agent_df = pd.read_json(msgl.loc[0,'msg'])
                            coil_df.at[0,'budget'] = msgl.loc[0,'new_budget']
                            coil_df.at[0,'From'] = agent_df.loc[0,'loc']
                            coil_df.at[0,'oname'] = agent_df.loc[0,'code']
                            coil_df.at[0,'path'] = agent_df.loc[0,'ph']
                            coil_df.at[0,'sgrade'] = agent_df.loc[0,'sg']
                            coil_df.at[0,'ancho'] = agent_df.loc[0,'ancho']
                            coil_df.at[0,'espesor'] = agent_df.loc[0,'espesor']
                            coil_df.at[0,'largo'] = agent_df.loc[0,'largo']
                            coil_df.loc[0,'param_f']= agent_df.loc[0,'parF']
                            coil_df.loc[0,'ship_date']= agent_df.loc[0,'sdate']
                            coil_df.at[0,'st'] = agent_df.loc[0,'st']
                            # Updating the BR register
                            i     = globals.gcl_jid
                            addbr = coil_df.to_json(orient="records")
                            cl_to_br = asf.find_br(i,addbr,'update')
                            cl_brw = asf.contact_list_json(cl_to_br,globals.gbrw_jid)
                            await self.send(cl_brw)
                            cl_log = pd.DataFrame()
                            cl_log.loc[0,'purpose'] = 'BUDGET UPDATED'
                            cl_log.loc[0,'coil'] = i
                            cl_log.loc[0,'new_budget'] = coil_df.at[0,'budget']
                            cl_msg_log = asf.msg_to_log(cl_log.to_json(\
                                     orient="records"), my_dir)
                            await self.send(cl_msg_log)

                        if msgl.loc[0,'purpose'] == 'search':
                            #
                            # Answering current properties to browser.
                            st = pd.DataFrame([{\
                                 'Code':coil_df.loc[0,'name'],\
                                 'User name': coil_df.loc[0,'id'],\
                                 'From':coil_df.loc[0,'oname'],\
                                 'msg': coil_df.loc[0,'name'], \
                                 'Location': coil_df.loc[0,'From'],
                                 'Capacity': coil_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':coil_df.loc[0,'ancho'],\
                                 'espesor': coil_df.loc[0,'espesor'],\
                                 'largo': coil_df.loc[0,'largo'],\
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'sdate': coil_df.loc[0,'ship_date'],\
                                 'status': coil_status_var, \
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'sgrade': coil_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)
                            msg_sender_jid = ''

                        if msgl.loc[0,'purpose'] == 'status_coil':
                            #
                            # Answering current properties to browser.
                            st = pd.DataFrame([{\
                                 'Code':coil_df.loc[0,'name'],\
                                 'User name': coil_df.loc[0,'id'],\
                                 'From':coil_df.loc[0,'oname'],\
                                 'msg': coil_df.loc[0,'name'], \
                                 'Location': coil_df.loc[0,'From'],
                                 'Capacity': coil_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':coil_df.loc[0,'ancho'],\
                                 'espesor': coil_df.loc[0,'espesor'],\
                                 'largo': coil_df.loc[0,'largo'],\
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'sdate': coil_df.loc[0,'ship_date'],\
                                 'status': coil_status_var, \
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'sgrade': coil_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)

                        if msgl.loc[0,'purpose'] == 'exit':
                            #
                            i     = globals.gcl_jid
                            reg_cl = pd.DataFrame([{'id':i,'code':code,'loc':location,\
                                        'bdg':budget,'orden':ordr,'ph':args.path, \
                                        'ancho':str(int(ancho)),'sg':sgrd,\
                                        'espesor':str(float(esp)),'largo':str(int(largo)),\
                                        'st':'ini'}])
                            addbr = reg_cl.to_json(orient="records")
                            cl_to_br = asf.find_br(i,addbr,'delete')
                            cl_brw = asf.contact_list_json(cl_to_br,globals.gbrw_jid)
                            await self.send(cl_brw)
                            await self.unsubscribe(globals.gcl_jid)
                            time.sleep(1)
                            self.kill()
                        del msg
                    #
                    # Mr Barja's place for his code
                    elif "NW" in msg_sender_jid:
                        """Inform log """
                        msgbdy         = pd.read_json(msg.body)
                        nww_coil_msg_df = pd.read_json(msgbdy.loc[0,'msg'])
                        #
                        # Inform log
                        req = 'Answering NWW request'
                        seqc= 'Request from ' + msg_sender_jid + msgbdy.loc[0,'purpose']
                        coil_msg_log= asf.log_req_nww (my_full_name,req, seqc, \
                                        msg_sender_jid)
                        cl_msg_json = coil_msg_log.to_json(orient="records")
                        cl_msg_log  = asf.msg_to_log(cl_msg_json, my_dir)
                        await self.send(cl_msg_log)
                        #Evaluate initial conditions and location to see if coil enters auction
                        if msgbdy.loc[0,'purpose'] == 'invitation':
                            coil_df.at[0,'F_group']=asf.F_groups(coil_df.at[0,'param_f'], nww_coil_msg_df.at[0,'id'])
                            coil_enter = asf.nww_coil_enter_auction_rating(nww_coil_msg_df, coil_df, number_auction)

                        #
                        if bid_register_df.shape[0] > 0: # If the coil already passed
                            p0j = bid_register_df['agent_type'] == 'NWW'
                            p1j = bid_register_df['status'] == 'won'
                            if bid_register_df[p0j & p1j].shape[0] > 0:
                                p1j = bid_register_df['status'] == ''
                                bid_register_df.loc[p0j & p1j,'status'] = 'gone'
                        if msgbdy.loc[0,'purpose'] == 'invitation':
                            if bid_register_df.shape[0] > 0:
                                p0j = bid_register_df['agent_type'] == 'NWW'
                                p1j = bid_register_df['status'] == 'won'
                                if bid_register_df[p0j & p1j].shape[0] > 0:
                                    coil_enter = 0 # Already passed

                            if coil_enter > 0: # Bid suitable for offer
                                # Create bid
                                coil_bid = asf.nww_coil_bid(nww_coil_msg_df, coil_df, coil_status_var, coil_enter)
                                coil_df.loc[0,'bid'] = coil_bid
                                reg_bid = pd.DataFrame([{'id': my_full_name,\
                                        'Coil':my_full_name, \
                                        'idres':msg_sender_jid, 'agent_type':'NWW',\
                                        'auction_dt':datetime.datetime.now(),\
                                        'decision_dt': '',\
                                        'initial_bid':coil_bid,'second_bid':-1,\
                                        'accepted_bid':-1,'won_bid':-1,\
                                        'status':''}])
                                if bid_register_df.shape[0] > 0:
                                    bidx = bid_register_df.index[bid_register_df[\
                                            'idres']==msg_sender_jid]
                                    for ibid in bidx:
                                        if bid_register_df.loc[ibid,'status'] == '':
                                            bid_register_df.loc[ibid,'status'] = 'gone'
                                bid_register_df = pd.concat([bid_register_df, reg_bid],\
                                        ignore_index=True)
                                bid_register_df.set_index('id',drop=False)
                                if coil_df.loc[0,'init_nww_auction'] is None:
                                    coil_df.loc[0,'init_nww_auction'] = \
                                        datetime.datetime.now()
                                """ Inform log """
                                coil_msg_log_body = asf.send_to_nww_msg(\
                                        my_full_name, coil_bid, globals.glog_jid, '1')
                                cl_msg_log = coil_msg_log_body.to_json(\
                                        orient="records")
                                cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                                await self.send(cl_log)
                            else:  # Not interested in such RFQ
                                """ Inform log """
                                coil_msg_log_body = asf.send_to_nww_msg(\
                                        my_full_name, 'Not match: Not offering',\
                                        globals.glog_jid, '1')
                                cl_msg_log = coil_msg_log_body.to_json(\
                                        orient="records")
                                cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                                await self.send(cl_log)
                                coil_df.loc[0,'bid'] = 0
                            #
                            coil_df.loc[0,'budget_remaining'] = coil_df.loc[0,\
                                    'budget'] - coil_df.loc[0,'bid']
                            """ Send answer to NWW agent """
                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                        msg_sender_jid,'answer2invitation',\
                                        msgbdy.loc[0,'seq'])
                            cl_ans  = asf.contact_list_json(cl_agent,\
                                        msg_sender_jid)
                            await self.send(cl_ans)
                            coil_df.loc[0, 'number_auction'] = coil_df.loc[\
                                        0, 'number_auction'] + 1
                        #
                        if msgbdy.loc[0,'purpose'] == 'counterbid':
                            """ Receive request to counterbid """
                            counterbid = asf.nww_create_counterbid(\
                                        nww_coil_msg_df,coil_df, my_full_name)
                            if bid_register_df.shape[0] > 0:
                                p0j = bid_register_df['agent_type'] == 'NWW'
                                p1j = bid_register_df['status'] == 'won'
                                if bid_register_df[p0j & p1j].shape[0] > 0:
                                    counterbid = 0 # Already passed
                            """ Prepare bid to send to nww """
                            coil_df.loc[0, 'counterbid'] = counterbid
                            coil_df['User_name_nww'] = str(msg_sender_jid)
                            coil_df['budget_remaining'] = coil_df.loc[0, \
                                    'budget'] - coil_df.loc[0, 'counterbid']
                            p0j = bid_register_df.idres == msg_sender_jid
                            p1j = bid_register_df.second_bid == -1
                            pj  = bid_register_df.index[p0j & p1j]
                            bid_register_df.loc[pj,'second_bid'] = counterbid
                            """ Answering to the NWW """
                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                        msg_sender_jid,'answer2counterbid',\
                                                  msgbdy.loc[0,'seq'])
                            cl_ans  = asf.contact_list_json(cl_agent,\
                                        msg_sender_jid)
                            await self.send(cl_ans)
                            #
                            """ Inform log """
                            msg_lbdy = asf.send_to_nww_msg(\
                                    my_full_name,counterbid,globals.glog_jid,'2')
                            cl_msg_log = msg_lbdy.to_json(orient="records")
                            cl_log = asf.msg_to_log(cl_msg_log,my_dir)
                            await self.send(cl_log)
                            # print(' Answeing:'+msg.body)
                            # print(cl_agent)
                            # print('***')

                        if msgbdy.loc[0,'purpose'] == 'notprofit':
                            """ Receive request to counterbid """
                            bid_register_df.loc[max(bid_register_df.index),\
                                            'status'] = 'lost'
                            bid_register_df.loc[max(bid_register_df.index),\
                                            'decision_dt'] = datetime.datetime.now()
                            coil_status_var = 'stand-by'
                        #
                        if msgbdy.loc[0,'purpose'] == 'confirm':
                            """Receive request to confirm as winner """
                            """ Answering to the NWW """
                            # print(' Answeing Confirm:'+msg.body)
                            nww_coil_msg_df = pd.read_json(msgbdy.loc[0,'msg'])
                            # print(nww_coil_msg_df)
                            # print('===')
                            #
                            if nww_coil_msg_df.at[0, 'bid_status'] == 'acceptedbid':
                                # Store accepted Bid from ca agent
                                bid_register_df = asf.update_bid_nww(bid_register_df,nww_coil_msg_df)
                                #
                                p0j = bid_register_df.idres== msg_sender_jid
                                p1j = bid_register_df.accepted_bid == -1
                                pj  = bid_register_df.index[p0j & p1j]
                                ppj = -1
                                for ipj in pj:
                                    if bid_register_df.loc[ipj,'status'] == '':
                                        ppj = ipj
                                if ppj > -1:
                                    bid_register_df.loc[ppj,'accepted_bid'] = \
                                            bid_register_df.loc[ppj,'second_bid']
                                    bid_register_df.loc[ppj,'seq']= msgbdy.loc[0,'seq']
                                else:
                                    aid  = nww_coil_msg_df.at[0, 'id']
                                    astr = f'ID: {aid}. Not opened bids in the register:'
                                    astr = astr + f' Coil: {my_full_name}'
                                    coil_msg_log_body = asf.inform_log(my_full_name,\
                                                        astr,globals.glog_jid)
                                    cl_msg_lg_bd = coil_msg_log_body.to_json(orient="records")
                                    coil_msg_log = asf.msg_to_log(cl_msg_lg_bd, my_dir)
                                    await self.send(coil_msg_log)
                                #
                                # Pending bids from different NWW plants ???
                                p0j = bid_register_df.agent_type == 'NWW'
                                p2j = bid_register_df.status == ''
                                p3j = bid_register_df.idres != msg_sender_jid
                                pj  = bid_register_df.index[p0j & p1j & p2j & p3j]
                                if sum(pj) == 0: # We can decide as no additional
                                                 # invitations are pending
                                    pj = bid_register_df.index[p0j & p2j]
                                    minacc=bid_register_df.loc[pj,'accepted_bid'].min()
                                    p3j= bid_register_df.accepted_bid==minacc
                                    p1j= bid_register_df['status'] == 'won'
                                    pjs= bid_register_df.index[p0j & p2j & p3j]
                                    if bid_register_df[p0j & p1j].shape[0] > 0:
                                        p1j = bid_register_df['status'] == ''
                                        bid_register_df.loc[p0j & p1j & p3j,'status'] = 'gone'
                                        pjs= []
                                    for icl in pj:
                                        plant_id = bid_register_df.loc[icl,'idres']
                                        sqf = bid_register_df.loc[icl,'seq']
                                        if icl not in pjs: # Offers not winning
                                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                                            plant_id,'NOTacceptedbid',sqf)
                                            cl_ans  = asf.contact_list_json(cl_agent,\
                                                            plant_id)
                                            await self.send(cl_ans)
                                            bid_register_df.loc[icl,'status'] = 'lost'
                                        else:
                                            coil_df.loc[0, 'bid_status'] = 'acceptedbid'
                                            auction_finish_at = datetime.datetime.now()
                                            cl_msg_lbdy = asf.won_auction(my_full_name, \
                                                    sqf, auction_finish_at)
                                            coil_msg_js = asf.msg_to_log(cl_msg_lbdy, my_dir)
                                            await self.send(coil_msg_js)
                                            cl_agent= asf.rq_list(my_full_name,coil_df,\
                                                            plant_id,'OKacceptedbid',sqf)
                                            cl_ans  = asf.contact_list_json(cl_agent,\
                                                            plant_id)
                                            await self.send(cl_ans)
                                            bid_register_df.loc[icl,'status'] = 'won'
                                            # Changing the position of the coil and ending.
                                            globals.ipth = globals.ipth + 1
                                            if 'END' not in globals.pth:
                                                globals.pth.append('END')
                                            tgt = globals.pth[globals.ipth]
                                            if tgt == 'END': # Coil Ended
                                                if not math.isnan(coil_df.at[0,'counterbid']):
                                                    cbid = coil_df.loc[0,'counterbid']
                                                else:
                                                    cbid = coil_df.loc[0,'bid']
                                                dt_end = datetime.datetime.now().strftime(\
                                                            "%Y-%m-%d %H:%M:%S")
                                                msg_str = f'ENDEDUP: {my_full_name}, code:{code} '
                                                msg_str = msg_str+f', order: {ordr}, by: {dt_end}'
                                                msg_str = msg_str+f', offer: {cbid}'
                                                cl_msg_lbdy = asf.inform_log(my_full_name,\
                                                            msg_str,globals.glog_jid)
                                                cl_msg_lg_bd= cl_msg_lbdy.to_json(orient="records")
                                                coil_msg_log= asf.msg_to_log(cl_msg_lg_bd, my_dir)
                                                await self.send(coil_msg_log)
                                                self.kill()
                            #
                            if nww_coil_msg_df.at[0, 'bid_status'] == 'rejectedbid':
                                pj = bid_register_df['idres']==msg_sender_jid and \
                                        bid_register_df['accepted_bid'] == -1 and \
                                        bid_register_df['status'] == ''
                                bid_register_df.loc[pj,'accepted_bid'] =  -10
                                bid_register_df.loc[pj,'won_bid'] =  -10
                        #
                        if msgbdy.loc[0,'purpose'] not in ['invitation','counterbid',\
                                                'confirm','notprofit']:
                            """inform log of status"""
                            # print(' ELSE:'+ msgbdy.loc[0,'purpose'])
                            number_auction += int(1)
                            number_astr = f'{my_full_name} did not enter {msg_sender_jid} auction '
                            number_astr = number_astr + 'because does not meet the '
                            number_astr = number_astr + 'requirements. Not_entered auction '
                            number_astr = number_astr + f'number: {number_auction}. Purpose:'
                            number_astr = number_astr + msgbdy.loc[0,'purpose']
                            coil_msg_log_body = asf.inform_log(my_full_name,\
                                                number_astr,globals.glog_jid)
                            cl_msg_lg_bd = coil_msg_log_body.to_json(orient="records")
                            coil_msg_log = asf.msg_to_log(cl_msg_lg_bd, my_dir)
                            await self.send(coil_msg_log)
                            coil_status_var = 'stand-by'
                        del msg
                    #
                    elif "BR" in msg_sender_jid:
                        # Answering current properties to browser.
                        msgl = pd.read_json(msg.body)
                        if msgl.loc[0,'purpose'] == 'search':
                            st = pd.DataFrame([{
                                 'Code':coil_df.loc[0,'name'],\
                                 'User name': coil_df.loc[0,'id'],\
                                 'From':coil_df.loc[0,'oname'],\
                                 'msg': coil_df.loc[0,'name'], \
                                 'Location': coil_df.loc[0,'From'],
                                 'Capacity': coil_df.loc[0,'budget'], \
                                 'purpose':'report', 'ancho':coil_df.loc[0,'ancho'],\
                                 'espesor': coil_df.loc[0,'espesor'],\
                                 'largo': coil_df.loc[0,'largo'],\
                                 'sdate': coil_df.loc[0,'sdate'],\
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'prev_st': coil_df.loc[0, 'prev_st'],\
                                 'sgrade': coil_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)
                        else:
                            st = pd.DataFrame([{
                                'Code':coil_df.loc[0,'name'],\
                                 'User name': coil_df.loc[0,'id'],\
                                 'From':coil_df.loc[0,'oname'],\
                                 'msg': coil_df.loc[0,'name'], \
                                 'Location': coil_df.loc[0,'From'],
                                 'Capacity': coil_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':coil_df.loc[0,'ancho'],\
                                 'espesor': coil_df.loc[0,'espesor'],\
                                 'largo': coil_df.loc[0,'largo'],\
                                 'parF': coil_df.loc[0,'param_f'],\
                                 'prev_st': coil_df.loc[0, 'prev_st'],\
                                 'sdate': coil_df.loc[0,'sdate'],\
                                 'sgrade':coil_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,globals.gbrw_jid)
                            await self.send(rep)
                            msg_sender_jid = ''
                    else:
                        """inform log"""
                        coil_msg_log= f'{my_full_name} did not receive any msg from VA. '
                        coil_msg_log= coil_msg_log + f'Agent in the last {wait_msg_time}'
                        coil_msg_log= coil_msg_log + f' s at {coil_status_var}.'
                        log_body    = asf.inform_log(my_full_name,\
                                            coil_msg_log,globals.glog_jid)
                        coil_msg_log = asf.msg_to_log(log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    coil_msg_log = f'{my_full_name} did not receive any msg in the '
                    coil_msg_log= coil_msg_log + f'last {auction_time}s '
                    coil_msg_log= coil_msg_log + f'at {coil_status_var}'
                    log_body = asf.inform_log(my_full_name,\
                                            coil_msg_log,globals.glog_jid)
                    msg_log = asf.msg_to_log(log_body.to_json(orient="records"), my_dir)
                    await self.send(msg_log)
            elif coil_status_var == "sleep":
                """wait for message from in case fabrication was interrupted"""
                interrupted_fab_msg = await self.receive(timeout=wait_msg_time)
                if interrupted_fab_msg:
                    interrupted_fab_msg_sender = interrupted_fab_msg.sender
                    if interrupted_fab_msg_sender[:-33] == "bro":
                        interrupted_fab_msg_df = pd.read_json(interrupted_fab_msg)
                        if interrupted_fab_msg_df.loc[0, 'int_fab'] == 1:
                            coil_df.loc[0, 'int_fab'] = 1
                            coil_status_var = "stand-by"
                            """inform log of issue"""
                            this_time = datetime.datetime.now()
                            coil_msg_log_body = f'{my_full_name} interrupted fab. Received that msg at {this_time}'
                            coil_msg_log_body = json.dumps(coil_msg_log_body)
                            coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                            await self.send(coil_msg_log)
                    else:
                        """inform log"""
                        coil_msg_log_body = f'{my_full_name} receive msg at {coil_status_var}, but not from browser'
                        coil_msg_log_body = asf.inform_log(my_full_name,coil_msg_log_body,globals.glog_jid)
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                    coil_msg_log_body = asf.inform_log(my_full_name,coil_msg_log_body,globals.glog_jid)
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
                    now_time = datetime.datetime.now()
                    tiempo = now_time - auction_finish_at
                    segundos = tiempo.seconds
                    if segundos > 50:
                        asf.change_jid(my_dir, my_full_name)
                        self.kill()
            elif coil_status_var == "stand-by":
                # Default status for setup
                # Sending advise to browser
                #
                coil_inform_json = asf.inform_log_df(my_full_name,'coil', coil_started_at, coil_status_var, coil_df).to_json(orient="records")
                coil_msg_brw = asf.msg_to_agnt(coil_inform_json, globals.gbrw_jid)
                await self.send(coil_msg_brw)
                # now it just changes directly to auction
                coil_status_var = "auction"
            else:
                """inform log of status"""
                coil_inform_json = asf.inform_log_df(my_full_name, 'coil',coil_started_at, coil_status_var, coil_df).to_json(orient="records")
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                coil_status_var = "stand-by"

        async def ask_exit(self):
            global va_status_var, number, coil_mdf, seq_va, coil_status_var, \
                        bid_register_df
            dtw = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            lcbs= []
            if coil_msgs_df.shape[0] > 0:
                lcbs= coil_msgs_df['id'].to_list()
            lcb2= []
            if coil_msgs_df2.shape[0] > 0:
                lcb2= coil_msgs_df2['id'].to_list()
            lcb3= []
            if coil_msgs_df3.shape[0] > 0:
                lcb3= coil_msgs_df3['id'].to_list()
            reg_cl = pd.DataFrame([{'id':my_full_name,'status':coil_status_var,\
                                    'auction':seq_va,'date':dtw,'plants_bid': \
                                    bid_register_df.to_json(),\
                                    'msg': 'Launcher requests to exit.'}])
            log_body    = asf.inform_log(my_full_name,\
                                reg_cl,globals.glog_jid)
            coil_msg_log = asf.msg_to_log(log_body, my_dir)
            await self.send(coil_msg_log)
            log_body    = asf.inform_log(my_full_name,\
                                all_auctions,globals.glog_jid)
            coil_msg_log = asf.msg_to_log(log_body, my_dir)
            await self.send(coil_msg_log)
            time.sleep(1)

        async def on_end(self):
            i     = globals.gcl_jid
            reg_cl = pd.DataFrame([{'id':i,'code':code,'loc':location,\
                        'bdg':budget,'orden':ordr,'ph':args.path, \
                        'ancho':str(int(ancho)),'sg':sgrd,\
                        'espesor':str(float(esp)),'largo':str(int(largo)),\
                        'sdate':ship_date,'st':'ini'}])
            addbr = reg_cl.to_json(orient="records")
            cl_to_br = asf.find_br(i,addbr,'delete')
            cl_brw = asf.contact_list_json(cl_to_br,globals.gbrw_jid)
            await self.send(cl_brw)
            self.presence.unsubscribe(globals.gcl_jid)
            self.presence.set_unavailable()
            await self.agent.stop()

        async def on_start(self):
            """inform log of start"""
            coil_msg_start = asf.send_activation_finish(my_full_name, \
                        ip_machine, 'start')
            coil_msg_start = asf.msg_to_log(coil_msg_start, my_dir)
            await self.send(coil_msg_start)
            """ Informing the Browser agent """
            i     = globals.gcl_jid
            reg_cl = pd.DataFrame([{'id':i,'code':code,'loc':location,\
                        'bdg':budget,'orden':ordr,'ph':args.path, \
                        'ancho':str(int(ancho)),'sg':sgrd,\
                        'espesor':str(float(esp)),'largo':str(int(largo)),\
                        'mydir':my_dir,'agent':i.split('@')[0],\
                        'parF': param_f,'sdate':ship_date,'st':'ini'}])
            addbr = reg_cl.to_json(orient="records")
            cl_to_br = asf.find_br(i,addbr,'create')
            la_brw = asf.contact_list_json(cl_to_br,globals.gbrw_jid)
            await self.send(la_brw)
            self.counter = 1

        async def unsubscribe(self,agnt):
            self.presence.unsubscribe(agnt)

    async def setup(self):
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=3)
        b = self.CoilBehav(period=3, start_at=start_at)  # periodic sender
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b)

if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='coil parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=3, help='agent_number: 1,2,3,4..')
    parser.add_argument('-v', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-w', '--wait_auction_time', type=int, metavar='', required=False, default=500, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='auction', help='status_var: on, stand-by, off')
    parser.add_argument('-b', '--budget', type=int, metavar='', required=False, default=1000, help='budget: in case of needed, budget can be increased')
    parser.add_argument('-l', '--location', type=str, metavar='', required=False, default='K', help='location: K')
    parser.add_argument('-c', '--code', type=str, metavar='', required=False, default='cO202106101', help='code: cO202106101')
    parser.add_argument('-o', '--order', type=str, metavar='', required=False, default='cO20210610', help='order: cO20210610')
    parser.add_argument('-ah', '--ancho', type=str, metavar='', required=False, default='950', help='ancho: 950')
    parser.add_argument('-sd', '--order_shdate', type=str, metavar='',required=False, default='2021-12-12', help='Specify the ship date of the order. Example: --sd "2021-11-01" ')
    parser.add_argument('-sg', '--sgrade', type=str, metavar='', required=False, default='X400', help='steelgrade: X400')
    parser.add_argument('-ll', '--lcoil', type=int, metavar='', required=False, default='20000', help='lcoil: 20000')
    parser.add_argument('-thk', '--thickness', type=str, metavar='', required=False, default='0.8', help='thickness: 0.8')
    parser.add_argument('-F', '--parameter_F', type=float, metavar='', required=False, default=10, help='parameter_F: 10-79')
    parser.add_argument('-pst', '--prev_station', type=str, metavar='', required=False, default='CA', help='Previous station (operation).Write between ",".Format:CA_03,BA_01,BA_02...')
    parser.add_argument('-ph', '--path', type=str, metavar='', required=False, default='VA0*', help='path: "NWW[3,4];VA0*"')
    parser.add_argument('-u', '--user_name', type=str, metavar='', required=False, help='User to the XMPP platform')  # JOM 10/10
    parser.add_argument('-p', '--user_passwd', type=str, metavar='', required=False, help='Passwd for the XMPP platform')  # JOM 10/10
    parser.add_argument('-lag', '--log_agnt_id', type=str, metavar='', required=False, help='User ID for the log agent')
    parser.add_argument('-bag', '--brw_agnt_id', type=str, metavar='', required=False, help='User ID for the browser agent')

    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = args.user_name
    wait_msg_time = args.wait_msg_time
    auction_time = args.wait_auction_time
    coil_started_at = datetime.datetime.now()
    coil_status_var = args.status
    location = args.location
    code = args.code
    path = args.path.split(';')
    ordr = args.order
    ancho= args.ancho
    esp  = args.thickness
    largo= args.lcoil
    sgrd = args.sgrade
    param_f = int(args.parameter_F)
    prev_station = args.prev_station
    ship_date = args.order_shdate
    globals.ipth= 0
    globals.pth = path
    #
    refresh_time = datetime.datetime.now() + datetime.timedelta(seconds=1)
    auction_finish_at = ""
    """Save to csv who I am"""
    coil_df = asf.set_agent_parameters_coil(my_dir, code, my_full_name, \
                    ancho, esp, largo, sgrd, location, ordr, param_f,\
                    prev_station, path, ship_date)
    coil_df.at[0, 'budget'] = args.budget
    budget = coil_df.loc[0, 'budget']
    bid_register_df = pd.DataFrame()
    number_auction = int(0)
    coil_df.at[0,'number_auction'] = number_auction
    coil_df.loc[0,'init_va_auction'] = None
    coil_df.loc[0,'init_nww_auction'] = None
    seq_coil = int(200)
    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]
    globals.IP = ip_machine

    """XMPP info"""
    if hasattr(args,'log_agnt_id') :
        glog_jid = args.log_agnt_id
        globals.glog_jid = glog_jid
    if hasattr(args,'brw_agnt_id') :
        gbrw_jid = args.brw_agnt_id
        globals.gbrw_jid = gbrw_jid
    if len(args.user_name) > 0:
        coil_jid = args.user_name
        globals.gcl_jid = coil_jid
    else:
        coil_jid = asf.agent_jid(my_dir, my_full_name)
    if len(args.user_passwd) > 0:
        coil_passwd = args.user_passwd
    else:
        coil_passwd = asf.agent_passwd(my_dir, my_full_name)

    #
    salir = 0
    globals.tosend= []
    coil_agent = CoilAgent(coil_jid, coil_passwd)
    future = coil_agent.start(auto_register=True)
    future.result()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time and \
            coil_agent.is_alive() and salir == 0:
        time.sleep(1)
    else:
        coil_status_var = "off"
        coil_agent.stop()
    quit_spade()
