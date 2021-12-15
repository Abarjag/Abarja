import time, datetime, sys, os, argparse,json, re
import socket, globals, random, pdb
import pandas as pd
import numpy as np
import math
import operative_functions as asf
from spade import quit_spade
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message

global va_msg_log, auction_start, pre_auction_start

class TemperRollingAgent(Agent):
    class NWWBehav(CyclicBehaviour):
        async def run(self):
            global process_df, nww_status_var, my_full_name, nww_status_started_at, \
                   stop_time, my_dir, wait_msg_time, nww_data_df, nww_prev_coil_df, \
                   lot_size, auction_df, fab_started_at, leeway, op_times_df, \
                   auction_start, nww_to_tr_df, transport_needed, warehouse_needed, seq_nww, \
                   results_2, coil_msgs_df, medias_list, ip_machine, number, \
                   list_stg_coils, nww_msg_log, pre_auction_start, pending_au, \
                   coil_msgs_df2,coil_msgs_df3,post_auction_step, coil_notified, \
                   rep_pauction, all_auctions
            #
            if nww_status_var == "pre-auction":
                diff=(datetime.datetime.now()-pre_auction_start).total_seconds()
                # print(' Preauction: ' + str(self.act_qry('ask-coils','pre-auction')) +\
                #                             ' Sncds:'+ str(diff))
                rep_pauction = 0 # Tolerance of 5 to repetition in auctions.
                if self.act_qry('ask-coils','pre-auction') == 0 and diff > 25:
                    seq_nww = seq_nww + 1
                    auction_df.at[0, 'pre_auction_start'] = pre_auction_start
                    """Asks browser for active coils and locations"""
                    pre_auction_start = datetime.datetime.now()
                    [id_org,cl_agent] = await self.ask_coils()
                    curr = datetime.datetime.now()
                    globals.tosend.append({'idorg':id_org,'agnt_org': \
                                globals.gnww_jid,'act':'ask-coils',\
                                'agnt_dst':globals.gbrw_jid,'dt':curr,\
                                'st':'pre-auction'})
                #
                """ Process the messages """
                msg_ag = await self.receive(timeout=wait_msg_time)
                if msg_ag:

                    recl = re.match(r'^c\d+',str(msg_ag.sender))
                    # print(globals.ret_dact)
                    if 'BROW' in str(msg_ag.sender).upper():
                        cl_df = pd.read_json(msg_ag.body)
                        seqce = cl_df.loc[0,'seq']
                        [act,st] = self.ret_agnt(seqce)
                        if st == 'pre-auction' and act == 'ask-coils':
                            dact  = pd.read_json(cl_df.loc[0,'msg'])
                            glist = self.del_agnt(seqce,globals.tosend)
                            globals.tosend = glist
                            br_data_df = []
                            myfnam = (my_full_name.split('@')[0]).upper()
                            myfnam = myfnam[:-3]
                            for k in dact.index:
                                res = re.match(dact.loc[k,'ph'],myfnam)
                                if res:
                                    if res.group(0) == myfnam:
                                        br_data_df.append(dact.loc[k,'id'])
                            #
                            if len(br_data_df) > 0: # There are coils waiting ...

                                auction_df.at[0, 'active_coils'] = br_data_df
                                #
                                # initial auction level
                                nww_data_df.at[0, 'auction_level'] = 1
                                nww_data_df.at[0, 'bid_status'] = 'bid'
                                nww_to_coils_df = asf.nww_to_coils_initial_df(nww_data_df, nww_prev_coil_df, lot_size)
                                # json to send to coils with auction info
                                # including last dimensional values
                                nww_to_coils_json = nww_to_coils_df.to_json()
                                # Create a loop to inform of auctionable
                                # resource to willing to be fab coils.
                                jid_list = br_data_df
                                auction_df.at[0, 'number_preauction'] = auction_df.at[0, \
                                            'number_preauction'] + 1
                                number = int(auction_df.at[0, 'number_auction'])
                                """Inform log """
                                nww_log_msg = asf.send_nww(my_full_name, number,  \
                                            nww_data_df.at[0, 'auction_level'], jid_list)
                                nww_log_json= nww_log_msg.to_json(orient="records")
                                nww_msg_log = asf.msg_to_log(nww_log_json, my_dir)
                                await self.send(nww_msg_log)
                                nww_msg_to_coils = asf.nww_msg_to(nww_to_coils_json,\
                                            globals.gnww_jid)
                                for z in jid_list:
                                    # Ask Coils
                                    id_org  = int(random.random()*10000)
                                    cl_agent= asf.rq_list(my_full_name,nww_to_coils_df,\
                                            z,'invitation',id_org)
                                    cl_ans  = asf.contact_list_json(cl_agent,z)
                                    await self.send(cl_ans)
                                    curr = datetime.datetime.now()
                                    globals.tosend.append({'idorg':id_org,'agnt_org': \
                                            globals.gnww_jid,'act':'invitation',\
                                            'agnt_dst':z,'dt':curr,'st':'pre-auction'})
                                # print(' From BR: ')
                                # print(globals.tosend)
                    elif recl is not None: # Means we have a coil answer ...
                        # print('Message from coil ...')
                        # print(globals.tosend)
                        # print('=======')
                        coil_jid = str(msg_ag.sender)
                        msg_snd_jid = coil_jid.split('/')[0]
                        cl_df = pd.read_json(msg_ag.body)
                        if cl_df.loc[0,'purpose'] == 'answer2invitation':
                            seqce = cl_df.loc[0,'seq']
                            coil_msg_df = pd.read_json(cl_df.loc[0,'msg'])
                            coil_msg_df.at[0,'coil_jid'] = msg_snd_jid
                            coil_msgs_df = coil_msgs_df.append(coil_msg_df)  # received msgs
                            coil_msgs_df.reset_index(drop=True,inplace=True)
                            glist = self.del_agnt(seqce,globals.tosend)
                            globals.tosend = glist
                            [mindt,ref] = self.min_dt('invitation','pre-auction')
                            #
                            # print(' Remaining: ' + str(self.act_qry('invitation','pre-auction')))
                            # print(globals.tosend)
                            if self.act_qry('invitation','pre-auction') == 0 or \
                                    (ref-mindt).total_seconds() > 15:
                                # Time for resolving the pre-auction
                                nww_status_var = "auction"
                                auction_start = datetime.datetime.now() - \
                                    datetime.timedelta(seconds=70)
                                auction_df.at[0, 'auction_start'] = None
                                coil_msgs_df2 = pd.DataFrame() # Preparing the auction
                                # print(' Pasamos a AUCTION')
                                # print(' .................')
                        else:
                            """Inform log """
                            msgerr = cl_df.loc[0,'purpose']
                            nww_msg_log_body = f'{my_full_name} received answer ' + \
                                    f'from {msg_snd_jid} a special msg: {msgerr}.'
                            nww_df = pd.DataFrame()
                            nww_df.loc[0,'purpose'] = 'inform_error'
                            nww_df.loc[0,'msg'] = nww_msg_log_body
                            nww_msg_log = asf.msg_to_log(nww_df.to_json(\
                                     orient="records"), my_dir)
                            await self.send(nww_msg_log)
                            # print(' Unexpected: ' + cl_df.loc[0,'purpose'])
                    elif 'LAUN' in str(msg_ag.sender).upper():
                        # Message from Launcher requesting parameter update ...
                        msgl = pd.read_json(msg_ag.body)
                        msg_sender_jid = str(msg_ag.sender).split('/')[0]
                        if msgl.loc[0,'purpose'] == 'exit':
                            await self.ask_exit() # To log ...
                            self.kill() # To end.
                        elif msgl.loc[0,'purpose'] == 'search':
                            id_ag = msgl.loc[0,'seq']
                            cl_ag = asf.rq_list(my_full_name, all_auctions, \
                                         msg_sender_jid,'history',id_ag)
                            cnt_lst = asf.contact_list_json(cl_ag,msg_sender_jid)
                            await self.send(cnt_lst)
                        elif msgl.loc[0,'purpose'] == 'status_nww':
                            #
                            # Answering current properties to browser.
                            st = pd.DataFrame([{\
                                 'Code':nww_data_df.loc[0,'id'],\
                                 'From':nww_data_df.loc[0,'oname'],\
                                 'msg': nww_data_df.loc[0,'name'], \
                                 'Location': nww_data_df.loc[0,'From'],
                                 'Capacity': nww_data_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':nww_data_df.loc[0,'ancho'],\
                                 'espesor': nww_data_df.loc[0,'espesor'],\
                                 'largo': nww_data_df.loc[0,'largo'],\
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sdate': nww_data_df.loc[0,'ship_date'],\
                                 'status': nww_status_var, \
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sgrade': nww_data_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)
                #
                # else:
                #     print(' What is this?')
                #     print('Preauction and not message to read')
                #     print(self.act_qry('ask-coils','pre-auction'))
                #     print(' Diff:' + str(diff))
                #     print('===***===')
            if nww_status_var == "auction":
                diff=(datetime.datetime.now()-auction_start).total_seconds()
                # print(' Auction: ' + str(self.act_qry('counterbid','auction')) +\
                #                             ' Sncds:'+ str(diff))
                if self.act_qry('counterbid','auction') == 0 and \
                        coil_msgs_df.shape[0] > 0 and diff > 25:
                    if auction_df.at[0, 'auction_start'] == None:
                        auction_df.at[0, 'auction_start'] = auction_start
                        auction_df.at[0, 'number_auction'] = auction_df.at[\
                                0, 'number_auction'] + 1
                    number = int(auction_df.at[0, 'number_auction'])
                    bid_list = coil_msgs_df.loc[:, 'id'].tolist()
                    bid_list_msg = str(bid_list)
                    nww_data_df.at[0, 'auction_level'] = 2
                    """Evaluating coil bids"""
                    coil_msgs_df = coil_msgs_df[coil_msgs_df['bid'] > 0]
                    coil_msgs_df = coil_msgs_df.reset_index(drop=True)
                    auction_df.at[0, 'auction_coils'] = [str(coil_msgs_df['id'\
                                ].to_list())]
                    jid_list = coil_msgs_df['id'].to_list()
                    if len(jid_list) > 0:
                        bid_coil = asf.nww_bid_evaluation(coil_msgs_df, nww_data_df,'bid')
                        bid_coil = asf.nww_negotiate(bid_coil, coil_msgs_df)
                        bid_coil['bid_status'] = 'counterbid'
                        jid_list = bid_coil.loc[:, 'coil_jid'].tolist()
                        result = asf.nww_result(bid_coil, jid_list,'bid')

                        """Coil order,Biggest difference (bid-min price), bid, minimum price. To Log"""
                        diff = bid_coil['difference'].to_list()
                        bids = bid_coil['bid'].to_list()
                        minp = bid_coil['minimum_price'].to_list()
                        nww_df = pd.DataFrame()
                        nww_df.loc[0, 'Line'] = my_full_name.upper()
                        nww_df.loc[0, 'Level'] = 'BID'
                        nww_df.loc[0,'Positions'] = json.dumps(jid_list)
                        nww_df.loc[0,'Difference'] = json.dumps(diff)
                        nww_df.loc[0,'Bid'] = json.dumps(bids)
                        nww_df.loc[0,'Minimum_price'] = json.dumps(minp)
                        nww_msg_log = asf.msg_to_log(nww_df.to_json(\
                                 orient="records"), my_dir)
                        await self.send(nww_msg_log)

                    if len(result['Coil'].to_list()) >= 2:
                        for z in jid_list:
                            """Inform log """
                            nww_msg_body = asf.send_nww(my_full_name, number, \
                                        nww_data_df.at[0, 'auction_level'], jid_list)
                            nww_msg_bdjs = nww_msg_body.to_json(orient="records")
                            nww_msg_log = asf.msg_to_log(nww_msg_bdjs, my_dir)
                            await self.send(nww_msg_log)
                            """Ask coils for counterbid"""
                            id_org  = int(random.random()*10000)
                            cl_agent= asf.rq_list(my_full_name,bid_coil,\
                                    z,'counterbid',id_org)
                            cl_ans  = asf.contact_list_json(cl_agent,z)
                            await self.send(cl_ans)
                            curr = datetime.datetime.now()
                            globals.tosend.append({'idorg':id_org,'agnt_org': \
                                    globals.gnww_jid,'act':'counterbid',\
                                    'agnt_dst':z,'dt':curr,'st':'auction'})
                    else:
                        nww_status_var = "post-auction"
                        pending_au    = True
                        post_auction_step = datetime.datetime.now() - \
                            datetime.timedelta(seconds=70)
                        coil_msgs_df2 = result
                        results_2 = result
                        results_2.loc[0, 'Profit'] = results_2.loc[0, 'Difference']
                        results_2.loc[0, 'Counterbid'] = results_2.loc[0, 'Bid']
                        """Inform coil of assignation and agree on assignation"""
                        coil_notified = -1 # Not yet communicated
                #
                """ Process the messages """
                msg_ag = await self.receive(timeout=wait_msg_time)
                if msg_ag:
                    cl_df = pd.read_json(msg_ag.body)
                    lstids= [i['idorg'] for i in globals.tosend]
                    # print('\n***\n   Auction MSG:' + msg_ag.body)
                    # print(str(cl_df.loc[0,'seq']) + ' in '+' '.join(str(e) for e in lstids) + ' ? ')
                    recl = re.match(r'^c\d+',str(msg_ag.sender).split('@')[0]) # Message from coils
                    if recl is not None and cl_df.loc[0,'seq'] in lstids: # Message from coil agents
                        #
                        # print(msg_ag.body)
                        # print(globals.tosend)
                        # print(' -------------\n')
                        coil_jid = str(msg_ag.sender)
                        msg_snd_jid = coil_jid.split('/')[0]
                        cl_df = pd.read_json(msg_ag.body)
                        if cl_df.loc[0,'purpose'] == 'answer2counterbid':
                            seqce = cl_df.loc[0,'seq']
                            coil_msg_df2 = pd.read_json(cl_df.loc[0,'msg'])
                            coil_msg_df2.at[0,'coil_jid'] = msg_snd_jid
                            coil_msgs_df2 = coil_msgs_df2.append(coil_msg_df2)
                            coil_msgs_df2.reset_index(drop=True,inplace=True)
                            glist = self.del_agnt(seqce,globals.tosend)
                            globals.tosend = glist
                            [mindt,ref] = self.min_dt('counterbid','auction')
                            dlt = (ref-mindt).microseconds / 1.e+6
                            # print('Remaining: ' + str(len(globals.tosend)))
                            if len(globals.tosend) == 0 or dlt > 15:
                                # Time for resolving the auction
                                if coil_msgs_df2.shape[0] > 0:
                                    nww_status_var = "post-auction"
                                    pending_au    = True
                                    post_auction_step = datetime.datetime.now() - \
                                        datetime.timedelta(seconds=70)
                                    counterbid_coil = asf.nww_bid_evaluation(coil_msgs_df2, \
                                                nww_data_df,'counterbid')

                                    """Coil order,Profit, counterbid, minimum price. To Log"""
                                    coils = counterbid_coil['coil_jid'].to_list()
                                    diff = counterbid_coil['profit'].to_list()
                                    bids = counterbid_coil['counterbid'].to_list()
                                    minp = counterbid_coil['minimum_price'].to_list()
                                    nww_df = pd.DataFrame()
                                    nww_df.loc[0, 'Line'] = my_full_name.upper()
                                    nww_df.loc[0, 'Level'] = 'COUNTERBID'
                                    nww_df.loc[0,'Positions'] = json.dumps(coils)
                                    nww_df.loc[0,'Difference'] = json.dumps(diff)
                                    nww_df.loc[0,'Counterbid'] = json.dumps(bids)
                                    nww_df.loc[0,'Minimum_price'] = json.dumps(minp)
                                    nww_msg_log = asf.msg_to_log(nww_df.to_json(\
                                             orient="records"), my_dir)
                                    await self.send(nww_msg_log)

                                    """Inform coil of assignation and agree on assignation"""
                                    jid_list_2= counterbid_coil.loc[:,'coil_jid'].tolist()
                                    results_2 = asf.nww_result(counterbid_coil, \
                                                jid_list_2,'counterbid')
                                    coil_notified = -1 # Not yet communicated
                                else:
                                    nww_status_var == "stand-by"
                        else:
                            # print('What ???')
                            """Inform log """
                            msgerr = cl_df.loc[0,'purpose']
                            nww_msg_log_body = f'{my_full_name} received answer ' + \
                                    f'from {msg_snd_jid} a special msg: {msgerr}.'
                            nww_df = pd.DataFrame()
                            nww_df.loc[0,'purpose'] = 'inform_error'
                            nww_df.loc[0,'msg'] = nww_msg_log_body
                            nww_msg_log = asf.msg_to_log(nww_df.to_json(\
                                     orient="records"), my_dir)
                            await self.send(nww_msg_log)

                    elif 'LAUN' in str(msg_ag.sender).upper():
                        # Message from Launcher requesting parameter update ...
                        msgl = pd.read_json(msg_ag.body)
                        msg_sender_jid = str(msg_ag.sender).split('/')[0]
                        if msgl.loc[0,'purpose'] == 'exit':
                            await self.ask_exit() # To log ...
                            self.kill() # To end.
                        elif msgl.loc[0,'purpose'] == 'search':
                            #
                            id_ag = msgl.loc[0,'seq']
                            cl_ag = asf.rq_list(my_full_name, all_auctions, \
                                         msg_sender_jid,'history',id_ag)
                            cnt_lst = asf.contact_list_json(cl_ag,msg_sender_jid)
                            await self.send(cnt_lst)
                        elif msgl.loc[0,'purpose'] == 'status_nww':
                            #
                            # Answering current properties to browser.
                            st = pd.DataFrame([{\
                                 'Code':nww_data_df.loc[0,'id'],\
                                 'From':nww_data_df.loc[0,'oname'],\
                                 'msg': nww_data_df.loc[0,'name'], \
                                 'Location': nww_data_df.loc[0,'From'],
                                 'Capacity': nww_data_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':nww_data_df.loc[0,'ancho'],\
                                 'espesor': nww_data_df.loc[0,'espesor'],\
                                 'largo': nww_data_df.loc[0,'largo'],\
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sdate': nww_data_df.loc[0,'ship_date'],\
                                 'status': nww_status_var, \
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sgrade': nww_data_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)

            if nww_status_var == "post-auction":
                diff=(datetime.datetime.now()-post_auction_step).total_seconds()
                # print(' Post-Auction: ' + str(coil_msgs_df2.shape[0]) +\
                #                             ' Sncds:'+ str(diff))
                # print(globals.tosend)
                if coil_notified < (coil_msgs_df2.shape[0]-1) and pending_au and \
                                coil_msgs_df2.shape[0] > 0  and diff > 25 :
                    coil_notified    = coil_notified + 1
                    i = coil_msgs_df2.index[coil_notified]
                    """Evaluate extra bids and give a rating"""
                    nww_data_df.loc[0, 'auction_level'] = 3  # third level
                    coil_jid_winner_f= results_2.loc[i,'Coil']
                    coil_jid_winner  = coil_jid_winner_f.split('@')[0]
                    winner_df = results_2.loc[i:i,:]
                    winner_df = winner_df.reset_index(drop=True)
                    profit    = float(results_2.loc[i, 'Profit'])
                    post_auction_step= datetime.datetime.now()   # Reset to follow up
                    # print(' coil_notif:' + str(coil_notified))
                    #
                    if profit >= 0.1:
                        winner_df.at[0, 'bid_status'] = 'acceptedbid'
                        nww_data_df.at[0,'bid_status'] = 'acceptedbid'
                        nww_data_df.loc[0,'accumulated_profit'] = nww_data_df.loc[0,\
                                   'accumulated_profit'] + winner_df.loc[0, 'Profit']
                        """Inform log """
                        nww_log_msg = asf.send_nww(my_full_name, number, \
                                    nww_data_df.at[0, 'auction_level'], \
                                    coil_jid_winner_f)
                        nww_log_json= nww_log_msg.to_json(orient="records")
                        nww_msg_log = asf.msg_to_log(nww_log_json, my_dir)
                        await self.send(nww_msg_log)
                        """ Ask winner coil for OK """
                        id_org  = int(random.random()*10000)
                        cl_agent= asf.rq_list(my_full_name,winner_df,\
                                    coil_jid_winner,'confirm',id_org)
                        cl_ans  = asf.contact_list_json(cl_agent,coil_jid_winner_f)
                        await self.send(cl_ans)
                        curr = datetime.datetime.now()
                        globals.tosend.append({'idorg':id_org,'agnt_org': \
                                    globals.gnww_jid,'act':'confirm',\
                                    'agnt_dst':coil_jid_winner,'dt':curr,\
                                    'st':'post-auction'})
                        pending_au = False
                        # print(" Confirming:")
                        # print(cl_agent)
                        # print(globals.tosend)
                    else:
                        """inform log of issue"""
                        nww_msg_log_body = f'coil {coil_jid_winner_f} does not bring '
                        nww_msg_log_body = nww_msg_log_body + f'positive benefit to {my_full_name}'
                        nww_msg_log_body = asf.inform_error(nww_msg_log_body)
                        nww_msg_log = asf.msg_to_log(nww_msg_log_body, my_dir)
                        await self.send(nww_msg_log)
                        id_org  = int(random.random()*10000)
                        cl_agent= asf.rq_list(my_full_name,winner_df,\
                                    coil_jid_winner_f,'notprofit',id_org)
                        cl_ans  = asf.contact_list_json(cl_agent,coil_jid_winner_f)
                        await self.send(cl_ans)
                        # print('not profit => '+ cl_ans.body)

                elif coil_notified == coil_msgs_df2.shape[0]-1:
                    """ Post auction ended """
                    # print('C_Notified: '+ str(coil_notified))
                    auction_df.at[0, 'number_auction_completed'] = auction_df.at[\
                                    0, 'number_auction_completed'] + 1
                    nww_status_var = "stand-by"
                    coil_msgs_df = coil_msgs_df.drop(coil_msgs_df.index)
                    coil_msgs_df2= coil_msgs_df2.drop(coil_msgs_df2.index)
                    coil_msgs_df3= coil_msgs_df3.drop(coil_msgs_df3.index)
                    globals.tosend = []
                    globals.ret_dact= 0
                    pending_au = True
                    all_auctions = pd.concat([all_auctions, process_df],\
                                        ignore_index=True)
                    process_df = pd.DataFrame([], columns=['fab_start', \
                                'processing_time', 'start_auction_before', \
                                'start_next_auction_at','setup_speed', \
                                'ancho','largo', 'espesor'])
                    process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + \
                                 datetime.timedelta(seconds=start_auction_before)
                    process_df.at[0,'setup_speed'] = 0.25 # Normal speed 15000 mm/min in m/s
                    process_df.at[0, 'start_auction_before'] = datetime.datetime.now()
                #
                msg_ag = await self.receive(timeout=wait_msg_time)
                if msg_ag:
                    # print( 'Post-auction MSG:' + msg_ag.body)
                    # print(globals.tosend)
                    recl = re.match(r'^c\d+',str(msg_ag.sender)) # Message from coils
                    if recl is not None: # Message from coil agents
                        coil_jid = str(msg_ag.sender)
                        msg_snd_jid = coil_jid.split('/')[0]
                        cl_df = pd.read_json(msg_ag.body)
                        seqce = cl_df.loc[0,'seq']
                        coil_msg_df3 = pd.read_json(cl_df.loc[0,'msg'])
                        # print(' => ')
                        #
                        if cl_df.loc[0, 'purpose'] == 'OKacceptedbid':
                            """Save winner information"""
                            auction_df.at[0, 'coil_ratings'] = [coil_msg_df3.to_dict(\
                                    orient="records")]  # Save information to auction df
                            """Calculate processing time"""
                            coil_msg_df3.loc[0,'setup_speed'] = speed
                            process_df = asf.set_process_df(process_df, coil_msg_df3, cl_df)
                            """Inform log of assignation and auction KPIs"""
                            if not math.isnan(coil_msg_df3.at[0,'counterbid']):
                                counterbid_win = coil_msg_df3.loc[0,'counterbid']
                            else:
                                counterbid_win = coil_msg_df3.loc[0,'bid']
                            auction_df.at[0, 'number_auction_completed'] = auction_df.at[\
                                        0, 'number_auction_completed'] + 1
                            number = int(auction_df.at[0, 'number_auction_completed'])
                            nww_msg_log_body = asf.auction_nww_kpis(nww_data_df, coil_msg_df3,\
                                        auction_df, process_df, winner_df)
                            nww_msg_log = asf.msg_to_log(nww_msg_log_body.to_json(\
                                        orient="records"), my_dir)
                            await self.send(nww_msg_log)


                            if (winner_df.loc[0,'F_group'] == nww_prev_coil_df.loc[0,'F_group']):
                                lot_size=lot_size + winner_df.loc[0,'peso']
                            else:
                                lot_size=int(0)

                            nww_prev_coil_df = nww_data_df[['largo','ancho','espesor','param_f','F_group']]

                            pft     = winner_df.loc[0,'Profit']
                            dtw     = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            idord   = coil_msg_df3.loc[0,'id']
                            msg_str = f'AU_ENDED:{my_full_name}, auction:{number} '
                            msg_str = msg_str+f', winner:{idord}, price:{counterbid_win}'
                            msg_str = msg_str+f', profit:{pft}, date: {dtw}'
                            #
                            cl_msg_lbdy = asf.inform_log(my_full_name,\
                                        msg_str,globals.glog_jid)
                            cl_msg_lg_bd= cl_msg_lbdy.to_json(orient="records")
                            coil_msg_log= asf.msg_to_log(cl_msg_lg_bd, my_dir)
                            await self.send(coil_msg_log)
                            #
                            # Message to cancel other waiting coils ...
                            if len(results_2)>1:
                                for k in coil_msg_df2.index:
                                    if coil_msgs_df2.loc[k,'id'] != coil_msg_df3.loc[0,'id']:
                                        idc = coil_msgs_df2.loc[k,'id']
                                        tsnd= globals.tosend
                                        idt = self.find_qry('agnt_dst',idc.split('@')[0])
                                        if idt:
                                            glist = self.del_agnt(idt,globals.tosend)
                                            globals.tosend = glist
                            nww_status_var = "stand-by"
                            coil_msgs_df = coil_msgs_df.drop(coil_msgs_df.index)
                            coil_msgs_df2= coil_msgs_df2.drop(coil_msgs_df2.index)
                            coil_msgs_df3= coil_msgs_df3.drop(coil_msgs_df3.index)
                            globals.tosend = []
                            globals.ret_dact= 0
                            pending_au = True
                            all_auctions = pd.concat([all_auctions, process_df],\
                                                ignore_index=True)
                            process_df = pd.DataFrame([], columns=['fab_start', \
                                        'processing_time', 'start_auction_before', \
                                        'start_next_auction_at','setup_speed', \
                                        'ancho','largo', 'espesor', 'param_f'])
                            process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + \
                                         datetime.timedelta(seconds=start_auction_before)
                            process_df.at[0,'setup_speed'] = 0.25 # Normal speed 15000 mm/min in m/s
                            process_df.at[0, 'start_auction_before'] = datetime.datetime.now()
                        #
                        elif cl_df.loc[0, 'purpose'] == 'NOTacceptedbid':
                            msg_str = f'{my_full_name} rejected the auction'
                            msg_str = msg_str + f' number: {number}'
                            cl_msg_lbdy = asf.inform_log(my_full_name,\
                                                msg_str,globals.glog_jid)
                            cl_msg_lg_bd = cl_msg_lbdy.to_json(orient="records")
                            coil_msg_log = asf.msg_to_log(cl_msg_lg_bd, my_dir)
                            await self.send(coil_msg_log)
                            pending_au = True
                        else:
                            # print(' ** Err: '+ str(len(globals.tosend))+ ' diff:'+ str(diff))
                            rep_pauction = rep_pauction + 1
                            if rep_pauction > 5:
                                nww_status_var = 'pre-auction'
                                rep_pauction = 0
                        glist = self.del_agnt(seqce,globals.tosend)
                        globals.tosend = glist
                        # print(' ** Err2: '+ str(len(globals.tosend)))
                        # print(globals.tosend)
                    elif 'LAUN' in str(msg_ag.sender).upper():
                        # Message from Launcher requesting parameter update ...
                        msgl = pd.read_json(msg_ag.body)
                        msg_sender_jid = str(msg_ag.sender).split('/')[0]
                        if msgl.loc[0,'purpose'] == 'exit':
                            await self.ask_exit() # To log ...
                            self.kill() # To end.
                        elif msgl.loc[0,'purpose'] == 'search':
                            #
                            id_ag = msgl.loc[0,'seq']
                            cl_ag = asf.rq_list(my_full_name, all_auctions, \
                                         msg_sender_jid,'history',id_ag)
                            cnt_lst = asf.contact_list_json(cl_ag,msg_sender_jid)
                            await self.send(cnt_lst)
                        elif msgl.loc[0,'purpose'] == 'status_nww':
                            #
                            # Answering current properties to browser.
                            st = pd.DataFrame([{\
                                 'Code':nww_data_df.loc[0,'id'],\
                                 'From':nww_data_df.loc[0,'oname'],\
                                 'msg': nww_data_df.loc[0,'name'], \
                                 'Location': nww_data_df.loc[0,'From'],
                                 'Capacity': nww_data_df.loc[0,'budget'], \
                                 'purpose':'report', \
                                 'ancho':nww_data_df.loc[0,'ancho'],\
                                 'espesor': nww_data_df.loc[0,'espesor'],\
                                 'largo': nww_data_df.loc[0,'largo'],\
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sdate': nww_data_df.loc[0,'ship_date'],\
                                 'status': nww_status_var, \
                                 'parF': nww_data_df.loc[0,'param_f'],\
                                 'sgrade': nww_data_df.loc[0,'sgrade']}]).to_json(\
                                            orient="records")
                            rep= asf.msg_to_agnt(st,msgl.loc[0,'id'])
                            await self.send(rep)
                        elif msgl.loc[0,'purpose'] == 'searchst':
                            #
                            id_ag = msgl.loc[0,'seq']
                            if nww_status_var == 'pre-auction':
                                dff = coil_msgs_df
                            if nww_status_var == 'auction':
                                dff = coil_msgs_df2
                            if nww_status_var == 'post-auction':
                                dff = coil_msgs_df3
                            cl_ag = asf.rq_list(my_full_name, dff, \
                                         msg_sender_jid,'history',id_ag)
                            cnt_lst = asf.contact_list_json(cl_ag,msg_sender_jid)
                            await self.send(cnt_lst)

                if diff > 160:
                    # After 10 mins without answer we drop off the auction
                    for k in coil_msgs_df2.index:
                        idc = coil_msgs_df2.loc[k,'id']
                        tsnd= globals.tosend
                        if len(tsnd) > 0:
                            ipc = self.find_qry('agnt_dst',idc.split('@')[0])
                            if ipc:
                                glist = self.del_agnt(ipc,globals.tosend)
                                globals.tosend = glist
                    nww_status_var = "stand-by"
                    coil_msgs_df = coil_msgs_df.drop(coil_msgs_df.index)
                    coil_msgs_df2= coil_msgs_df2.drop(coil_msgs_df2.index)
                    coil_msgs_df3= coil_msgs_df3.drop(coil_msgs_df3.index)
                    globals.tosend = []
                    globals.ret_dact= 0
                    all_auctions = pd.concat([all_auctions, process_df],\
                                        ignore_index=True)
                    pending_au = True
                    process_df = pd.DataFrame([], columns=['fab_start', 'processing_time', \
                                 'start_auction_before', 'start_next_auction_at',\
                                'setup_speed', 'ancho','largo', 'espesor','param_f'])
                    process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + \
                                 datetime.timedelta(seconds=start_auction_before)
                    process_df.at[0,'setup_speed'] = 0.25 # Normal speed 15000 mm/min in m/s
                    process_df.at[0, 'start_auction_before'] = datetime.datetime.now()
            #
            # stand-by status for VA is very useful. It changes to pre-auction.
            elif nww_status_var == "stand-by":
                """ Starts next auction when there is some time left
                    before current fab ends """
                if len(globals.tosend) == 0:
                    nww_status_var = 'pre-auction'
                if len(globals.tosend) > 0:
                    act = globals.tosend[0]['act']
                    if act == 'invitation':
                        nww_status_var = 'pre-auction'
                    elif act == 'counterbid':
                        nww_status_var = 'auction'
                # print(' stand-by => '+ va_status_var)

            else:
                """inform log of status"""
                if 'auction' not in nww_status_var:
                    nww_inform_json = asf.inform_log_df(my_full_name, 'nww',nww_status_started_at, nww_status_var).to_json(orient="records")
                    nww_msg_log = asf.msg_to_log(nww_inform_json, my_dir)
                    await self.send(nww_msg_log)
                    # print(' Unknown => '+ va_status_var)
                    # va_status_var = "stand-by"

        async def ask_exit(self):
            global nww_status_var, number, coil_msgs_df, coil_msgs_df2,\
                        coil_msgs_df3, all_auctions, seq_nww
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
            reg_cl = pd.DataFrame([{'id':globals.gnww_jid,'status':nww_status_var,\
                                    'auction':seq_nww,'date':dtw,'coils_bid': lcbs,\
                                    'coils_cbid': lcb2,'coils_solv': lcb3,\
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

        async def ask_coils(self):
            r = 'Request coil list'
            seqce    = int(random.random()*10000)
            rq_clist = asf.rq_list(my_full_name, r, globals.gbrw_jid,\
                                   'getlist',seqce)
            r_clist  = asf.contact_list_json(rq_clist,'browser')
            await self.send(r_clist)
            return([seqce,rq_clist])

        def ret_agnt(self,id_agnt):
            for idct in globals.tosend:
                if idct['idorg'] == id_agnt:
                    return([idct['act'], idct['st']])

        def del_agnt(self,id_agnt,glist):
            rem = -1
            for inum in range(len(glist)):
                idct = glist[inum]
                if idct['idorg'] == id_agnt:
                    rem = inum
            if rem > -1:
                glist.pop(rem)
            return(glist)

        def act_qry(self,act,st):
            i = 0
            for idct in globals.tosend:
                if idct['act'] == act and idct['st']==st:
                    i = i + 1
            return(i)

        def find_qry(self,field,val):
            for idct in globals.tosend:
                if str(val).upper() in str(idct[field]).upper():
                    return(idct['idorg'])
            return(None)

        def min_dt(self,act,st):
            low0 = datetime.datetime.now()
            low  = low0
            for idct in globals.tosend:
                if idct['act'] == act and idct['st'] == st:
                    low = min(idct['dt'],low)
            return([low,low0])

        async def on_end(self):
            va_msg_ended = asf.send_activation_finish(my_full_name, ip_machine, 'end')
            va_msg       = asf.msg_to_log(va_msg_ended, my_dir)
            await self.send(va_msg)
            await self.presence.unsubscribe(globals.gbrw_jid)
            await self.agent.stop()

        async def on_end(self):
            nww_msg_ended = asf.send_activation_finish(my_full_name, ip_machine, 'end')
            nww_msg       = asf.msg_to_log(nww_msg_ended, my_dir)
            await self.send(nww_msg)
            await self.presence.unsubscribe(globals.gbrw_jid)
            await self.agent.stop()

        async def on_start(self):
            global nww_msg_log, auction_start, pre_auction_start
            self.counter = 1
            coil_msgs_df  = pd.DataFrame()
            coil_msgs_df2 = pd.DataFrame()
            coil_msgs_df3 = pd.DataFrame()
            """Inform log """
            nww_msg_start = asf.send_activation_finish(my_full_name, \
                    ip_machine, 'start')
            nww_msg_log = asf.msg_to_log(nww_msg_start, my_dir)
            await self.send(nww_msg_log)
            nww_activation_json = asf.activation_df(my_full_name,\
                    nww_status_started_at,globals.gnww_jid)
            nww_msg_lg = asf.msg_to_log(nww_activation_json, my_dir)
            await self.send(nww_msg_lg)

    async def setup(self):
        # start_at = datetime.datetime.now() + datetime.timedelta(seconds=3)
        # b = self.NWWBehav(period=3, start_at=start_at)  # periodic sender
        b = self.NWWBehav()  # periodic sender
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b,template)

if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,3,4')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='pre-auction', help='status_var: on, stand-by, Off')
    parser.add_argument('-sab', '--start_auction_before', type=int, metavar='', required=False, default=10, help='start_auction_before: seconds to start auction prior to current fab ends')
    parser.add_argument('-sd', '--speed', type=float, metavar='', required=False, default=0.25, help='NWW speed. Example --speed 0.25 ')
    parser.add_argument('-tc', '--transport_agent', type=str, metavar='', required=False, default='no', help='transport_agent: yes, no')
    parser.add_argument('-wh', '--warehouse_agent', type=str, metavar='', required=False, default='no', help='wharehouse_agent: yes, no')
    # MANAGEMENT DATA
    parser.add_argument('-u', '--user_name', type=str, metavar='', required=False, help='User to the XMPP platform')  # JOM 10/10
    parser.add_argument('-p', '--user_passwd', type=str, metavar='', required=False, help='Passwd for the XMPP platform')  # JOM 10/10
    parser.add_argument('-lag', '--log_agnt_id', type=str, metavar='', required=False, help='User ID for the log agent')
    parser.add_argument('-bag', '--brw_agnt_id', type=str, metavar='', required=False, help='User ID for the browser agent')


    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    nww_status_started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nww_status_refresh = datetime.datetime.now() + datetime.timedelta(seconds=5)
    nww_status_var = args.status
    start_auction_before = args.start_auction_before
    speed = args.speed
    transport_needed= args.transport_agent
    warehouse_needed=args.warehouse_agent
    """Save to csv who I am"""
    nww_data_df = asf.set_agent_parameters(my_name, my_full_name,950,0.8,4950,40,'X500','F','')
    nww_prev_coil_df = nww_data_df[['largo','ancho','espesor','param_f', 'F_group']]
    nww_data_df['accumulated_profit'] = 0
    process_df = pd.DataFrame([], columns=['fab_start', 'processing_time', \
                 'start_auction_before', 'start_next_auction_at',\
                'setup_speed', 'ancho','largo', 'espesor','param_f'])
    process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + \
                 datetime.timedelta(seconds=start_auction_before)
    process_df.at[0,'setup_speed'] = 0.25 # Normal speed 15000 mm/min in m/s
    process_df.at[0, 'start_auction_before'] = datetime.datetime.now()
    fab_started_at = datetime.datetime.now()
    auction_df = asf.auction_blank_df()
    auction_df.at[0, 'number_preauction'] = 0
    auction_df.at[0, 'number_auction'] = 0
    auction_df.at[0, 'number_auction_completed'] = 0
    all_auctions = pd.DataFrame()
    leeway = datetime.timedelta(minutes=int(2))  # with fab process time ranging between 8-10 min, and tr op time between 3-4 min. Max dif between estimation and reality is 3min.
    op_times_df = pd.DataFrame([], columns=['AVG(nww_op_time)', 'AVG(tr_op_time)'])
    lot_size=int(0)
    seq_nww = int(0)
    nww_to_tr_df = pd.DataFrame()

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
    if hasattr(args,'lhr_agnt_id') :
        glhr_jid = args.lhr_agnt_id
        globals.glhr_jid = glhr_jid
    if len(args.user_name) > 0:
        nww_jid = args.user_name
    else:
        nww_jid = asf.agent_jid(my_dir, my_full_name)
    if len(args.user_passwd) > 0:
        nww_passwd = args.user_passwd
    else:
        nww_passwd = asf.agent_passwd(my_dir, my_full_name)
        nww_jid = asf.agent_jid(my_dir, my_full_name)

    globals.gnww_jid = nww_jid
    globals.tosend  = []
    globals.ret_dact= 0
    pre_auction_start = datetime.datetime.now() - datetime.timedelta(\
                        seconds=90)
    coil_msgs_df    = pd.DataFrame()
    coil_msgs_df2   = pd.DataFrame()
    coil_msgs_df3   = pd.DataFrame()
    auction_start   = datetime.datetime.now()
    #
    nww_agent = TemperRollingAgent(nww_jid, nww_passwd)
    future = nww_agent.start(auto_register=True)
    future.result()

    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        nww_status_var = "off"
        nww_agent.stop()
        quit_spade()
