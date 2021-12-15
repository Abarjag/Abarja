import argparse, os, time, datetime, random
import json, socket, globals, re, pdb
import operative_functions as opf
import pandas as pd
from spade import quit_spade
from spade.agent import Agent
from spade.behaviour import OneShotBehaviour
from spade.template import Template

# Global variable supporting the identity of the log agent
glog_jid = ''
gbrw_jid = ''

class LaunchAgent(Agent):
    class LABehav(OneShotBehaviour):
        async def run(self):
            global la_status_var, my_full_name, la_started_at, stop_time, \
                  my_dir, wait_msg_time, ip_machine, counter,df, order_code,\
                  name_coil, la_search, la_exit, answeryet, dact

            """ Start new coils submitting coil agents """
            if order_code != "No":
                la_inform_log = opf.order_file(my_full_name, order_code, \
                           steel_grade, thickness, width_coils, \
                           num_coils, list_coils, list_lengths, param_f, \
                           each_coil_price, list_ware, string_operations, \
                           prev_station, wait_msg_time, ship_date)
                la_inform_log_json = la_inform_log.to_json(orient="records")
                la_order_log = opf.order_to_log(la_inform_log_json, my_dir)
                await self.send(la_order_log)
                #
                # Active coil agents
                contact_list = await self.list_agnts()
                #
                # Active coil properties
                lnchr = globals.glhr_jid
                dact  = await self.list_coils()
                inform_log = opf.change_warehouse(la_inform_log,contact_list,dact)
                #
                # The new coils will notify Browser agent but changes need
                # specific notification to the coil agent by a message
                for ic in range(inform_log.shape[0]):
                    # change => Inform COIL agent
                    if inform_log.iloc[ic]['st'] == 'chg':
                        agid   = inform_log.iloc[ic]['id']
                        r      = '['+inform_log.iloc[ic].to_json()+']'
                        seqce  = int(random.random()*10000)
                        cl_ag  = opf.rq_list(my_full_name,r,agid,'update',seqce)
                        cnt_lst= opf.contact_list_json(cl_ag,agid)
                        await self.send(cnt_lst)
                        #
                order_code = 'No'

            """Change in order"""
            if name_coil != "No":
                dact  = await self.list_coils()
                la_coil_json = opf.order_budget(change_budget, name_coil, dact)
                la_coil  = opf.contact_list_json(la_coil_json,la_coil_json.loc[0, 'to'])
                await self.send(la_coil)
                la_coil_log  = opf.contact_list_json(la_coil_json, globals.glog_jid)
                await self.send(la_coil_log)
                name_coil = 'No'

            """Send searching code to browser"""
            if la_search != "No":   # --search aa=aglist / aa=list
                lnchr = globals.glhr_jid
                tcmd  = la_search.split('=')[0]
                pcmd  = la_search.split('=')[1]
                if tcmd == 'aa':
                    if pcmd.upper() == "LIST": # answering --search aa=list
                        dact = await self.list_coils()
                        print(dact.to_json(orient="records"))
                    elif pcmd.upper() == "AGLIST": # answering --search aa=aglist
                        contact_list = await self.list_agnts()
                        print(json.dumps(contact_list))
                    elif 'VA' in pcmd.upper():
                        lauct = await self.list_auctions(pcmd)
                        print(lauct.to_json(orient="records"))
                elif tcmd == 'st':
                    clist = pcmd.split(',')
                    cl_det= await self.coil_ask(lnchr,clist)
                    print(cl_det.to_json(orient="records"))
                elif tcmd == 'vst':
                    cl_det= await self.va_ask(lnchr,pcmd)
                    print(cl_det.to_json(orient="records"))
                elif tcmd == 'oc':
                    lsch  = pcmd.split(',')
                    dact  = await self.list_coils()
                    dactf = dact.loc[dact['From'].isin(lsch),:]
                    print(dactf.to_json(orient="records"))
                    dactf_json = dactf.to_json(orient="records")
                    la_orders_log = opf.order_to_log(dactf_json, my_dir)
                    await self.send(la_orders_log)
                #
                # other cases for search subcomands ...
                la_search = 'No'

            " Endif the work of a coil "
            if la_exit != "No":
                # Killing the agent
                lext = la_exit.split('=')[1].split(',')
                for iag in lext:
                    sig = pd.DataFrame([{'id':globals.glhr_jid,'purpose':\
                            'exit','to':iag}]).to_json(orient="records")
                    rqst = opf.msg_to_agnt(sig,iag)
                    await self.send(rqst)
                    la_exit = 'No'

            await self.agent.stop()

        def act_qry(self,act):
            i = 0
            for idct in globals.tosend:
                if idct['act'] == act:
                    i = i + 1
            return(i)

        def ret_agnt(self,id_agnt):
            for idct in globals.tosend:
                if idct['idorg'] == id_agnt:
                    return([idct['ag_to'],idct['purpose']])

        def del_agnt(self,id_agnt,glist):
            rem = -1
            for inum in range(len(glist)):
                idct = glist[inum]
                if idct['idorg'] == id_agnt:
                    rem = inum
            if rem > -1:
                glist.pop(rem)
            return(glist)

        async def coil_ask(self,launcher,clist):
            msg_df = pd.DataFrame()
            for i in clist:
                j = i[1:].split('@')[0]
                if j.isnumeric():
                    seqce  = int(random.random()*10000)
                    search = 'Search:GET_COIL_AGENT:'+i
                    coil_to_search = opf.find_br(launcher,search,'status_coil')
                    # We ask directly to the agent
                    coil_to_search['to']=i
                    cl_srch_json = coil_to_search.to_json(orient="records")
                    la_inform_cl = opf.msg_to_agnt(cl_srch_json,i)
                    await self.send(la_inform_cl)
                    time.sleep(5)
                    globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                            'ag_to':i,'purpose':'status_coil'})
                    msg_ar = await self.receive(timeout=wait_msg_time)
                    if msg_ar:
                        cl_df    = pd.read_json(msg_ar.body)
                        msg_sndr = str(msg_ar.sender).split('/')[0]
                        recl = re.match(r'^c\d+',str(msg_sndr))
                        if recl is not None and cl_df.loc[0,'purpose'] == 'report':
                            msg_df = pd.concat([msg_df,cl_df],axis=0, ignore_index=True)
                            glist = self.del_agnt(seqce,globals.tosend)
                            globals.tosend = glist
            return(msg_df)

        async def va_ask(self,launcher,i):
            msg_df = pd.DataFrame()
            j = i[2:].split('@')[0]
            if j.isnumeric() and 'VA' == j[0:2].upper():
                seqce  = int(random.random()*10000)
                search = 'Search:GET_VA_AGENT:'+j
                coil_to_search = opf.find_br(launcher,search,'status_va')
                # We ask directly to the agent
                coil_to_search['to']=i
                cl_srch_json = coil_to_search.to_json(orient="records")
                la_inform_cl = opf.msg_to_agnt(cl_srch_json,i)
                await self.send(la_inform_cl)
                time.sleep(5)
                globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                        'ag_to':i,'purpose':'status_va'})
                msg_ar = await self.receive(timeout=wait_msg_time)
                if msg_ar:
                    cl_df    = pd.read_json(msg_ar.body)
                    msg_sndr = str(msg_ar.sender).split('/')[0]
                    recl = re.match(r'^c\d+',str(msg_sndr))
                    if recl is not None and cl_df.loc[0,'purpose'] == 'report':
                        msg_df = pd.concat([msg_df,cl_df],axis=0, ignore_index=True)
                        glist = self.del_agnt(seqce,globals.tosend)
                        globals.tosend = glist
            return(msg_df)

        async def nww_ask(self,launcher,i):
            msg_df = pd.DataFrame()
            j = i[2:].split('@')[0]
            if j.isnumeric() and 'NWW' == j[0:2].upper():
                seqce  = int(random.random()*10000)
                search = 'Search:GET_NWW_AGENT:'+j
                coil_to_search = opf.find_br(launcher,search,'status_nww')
                # We ask directly to the agent
                coil_to_search['to']=i
                cl_srch_json = coil_to_search.to_json(orient="records")
                la_inform_cl = opf.msg_to_agnt(cl_srch_json,i)
                await self.send(la_inform_cl)
                time.sleep(5)
                globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                        'ag_to':i,'purpose':'status_nww'})
                msg_ar = await self.receive(timeout=wait_msg_time)
                if msg_ar:
                    cl_df    = pd.read_json(msg_ar.body)
                    msg_sndr = str(msg_ar.sender).split('/')[0]
                    recl = re.match(r'^c\d+',str(msg_sndr))
                    if recl is not None and cl_df.loc[0,'purpose'] == 'report':
                        msg_df = pd.concat([msg_df,cl_df],axis=0, ignore_index=True)
                        glist = self.del_agnt(seqce,globals.tosend)
                        globals.tosend = glist
            return(msg_df)

        async def coil_updt(self,clist):
            msg_df = pd.DataFrame()
            for i in range(clist.shape[0]):
                updt = 'Update:COIL_AGENT_METADATA:'+i
                coil_to_search_json = opf.find_br(my,updt,'search'\
                            ).to_json(orient="records")
                la_inform_cl = order_to_search(coil_to_search_json,clist.iloc[i,'id'],my_dir)
                await self.send(la_inform_cl)
                msg = await self.receive(timeout=5) # wait 5 secs
                msg_df = pd.concat([msg_df,pd.read_json('['+msg.body+']')],axis=0, ignore_index=True)
                if msg_df['status'] != 'done':
                    self.err_launc_coils(clist.iloc[i,'id'],clist.iloc[i,'code'],clist.iloc[i,'orden'])
            globals.cupdt = msg_df

        async def err_launc_coils(self,i,id,odn):
            # Report Error to LOG
            la_msg_rec = pd.DataFrame([],columns=['id','purpose','msg','order_code','string_operations','date'])
            la_msg_rec.at[0,'id'] = i
            la_msg_rec.at[0,'purpose'] = 'update coil params'
            la_msg_rec.at[0,'msg'] = "Coil code:"+id + "=>Fail update loc and budget in agent "+i
            la_msg_rec.at[0,'order_code'] = odn
            la_msg_rec.at[0,'string_operations'] = globals.string_operations
            la_msg_rec.at[0,'date'] = date.today().strftime('%Y-%m-%d')
            ord_msg = Message(to=globals.glog_jid)
            ord_msg.body = la_msg_rec.to_json(orient="records")
            ord_msg.set_metadata("performative","error")
            await self.send(ord_msg)

        async def list_coils(self):
            r        = 'Request coil list'
            dact     = pd.DataFrame()
            seqce    = int(random.random()*10000)
            rq_clist = opf.rq_list(my_full_name,r,globals.glog_jid,r,seqce)
            rclist   = opf.contact_list_json(rq_clist,'log')
            await self.send(rclist)
            dact     = pd.DataFrame()
            rq_clist = opf.rq_list(my_full_name, r, globals.gbrw_jid,\
                                   'getlist',seqce)
            r_clist  = opf.contact_list_json(rq_clist,'browser')
            await self.send(r_clist)
            globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                        'ag_to':globals.gbrw_jid,'purpose':'list_coils'})
            time.sleep(2)
            msg_ar   = await self.receive(timeout=wait_msg_time)
            if msg_ar:
                cl_df    = pd.read_json(msg_ar.body)
                msg_sndr = str(msg_ar.sender).split('/')[0]
                [who, purpose] = self.ret_agnt(cl_df.loc[0,'seq'])
                if 'brow' in msg_sndr and cl_df.loc[0,'purpose'] == 'getlist':
                    dact = pd.read_json(cl_df.loc[0,'msg'])
                    glist = self.del_agnt(cl_df.loc[0,'seq'],globals.tosend)
                    globals.tosend = glist
            return(dact)

        async def list_agnts(self):
            r = 'Request contact list'
            #
            contact_list = []
            seqce    = int(random.random()*10000)
            rq_clist = opf.rq_list(my_full_name,r,globals.glog_jid,r,seqce)
            rclist   = opf.contact_list_json(rq_clist,'log')
            await self.send(rclist)
            #
            rq_clist = opf.rq_list(my_full_name, r, globals.gbrw_jid,\
                        'contact_list',seqce)
            rclist   = opf.contact_list_json(rq_clist,'browser')
            await self.send(rclist)
            globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                        'ag_to':globals.gbrw_jid,'purpose':'list_agents'})
            time.sleep(2)
            msg_ar   = await self.receive(timeout=wait_msg_time)
            if msg_ar:
                cl_df    = pd.read_json(msg_ar.body)
                msg_sndr = str(msg_ar.sender).split('/')[0]
                [who, purpose] = self.ret_agnt(cl_df.loc[0,'seq'])
                if 'brow' in msg_sndr and cl_df.loc[0,'purpose'] == 'contact_list':
                    contact_list = json.loads(cl_df.loc[0, 'msg'])
                    glist = self.del_agnt(seqce,globals.tosend)
                    globals.tosend = glist
            return(contact_list)

        async def list_auctions(self,agnt):
            r        = 'Request auction list'
            seqce    = int(random.random()*10000)
            rq_clist = opf.rq_list(my_full_name, r, agnt,\
                                   'search',seqce)
            r_clist  = opf.contact_list_json(rq_clist,agnt)
            await self.send(r_clist)
            globals.tosend.append({'idorg':seqce,'ag_org':my_full_name,\
                        'ag_to':agnt,'purpose':'list_auctions'})
            time.sleep(2)
            lauct = pd.DataFrame()
            msg_ar   = await self.receive(timeout=wait_msg_time)
            if msg_ar:
                cl_df    = pd.read_json(msg_ar.body)
                msg_sndr = str(msg_ar.sender).split('/')[0]
                if 'brow' in msg_sndr and cl_df.loc[0,'purpose'] == 'history':
                    lauct = pd.read_json(cl_df.loc[0,'msg'])
                    glist = self.del_agnt(cl_df.loc[0,'seq'],globals.tosend)
                    globals.tosend = glist
            return(lauct)

    class ReceiverBehav(OneShotBehaviour):
        async def run(self):
            await self.agent.b.join()
            """Receive message"""
            msg = await self.receive(timeout=wait_msg_time) # wait for a message for 5 seconds
            if msg:
                msg_df = pd.read_json(msg.body)
                if msg_df.loc[0, 'purpose'] =="search_requested":
                    order_searched = msg_df.loc[0, 'msg']
                    print(order_searched)


        async def on_end(self):
            """Inform log """
            la_msg_ended = opf.send_activation_finish(my_full_name, ip_machine, 'end')
            la_msg_ended = opf.msg_to_log(la_msg_ended, my_dir)
            await self.send(la_msg_ended)
            await self.presence.unsubscribe(globals.glhr_jid)
            await self.agent.stop()

        async def on_start(self):
            """inform log of status"""
            la_activation_json = opf.activation_df(my_full_name, \
                                la_started_at, globals.glhr_jid)
            la_msg_log = opf.msg_to_log(la_activation_json, my_dir)
            await self.send(la_msg_log)
            self.counter = 1

    async def setup(self):
        b = self.LABehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)
        # self.b2 = self.ReceiverBehav()
        # template2 = Template()
        # template2.metadata = {"performative": "inform"}
        # self.add_behaviour(self.b2, template2)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=50, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=10, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, Off')
    parser.add_argument('--search', type=str, metavar='', required=False, default='No',help='Search order by code. Writte depending on your case: aa=list (list active agents), oc (order_code),sg(steel_grade),at(average_thickness), wi(width_coils), ic(id_coil), so(string_operations), date.Example: --search oc = 987')
    parser.add_argument('--exit', type=str, metavar='', required=False, default='No',help='Search order by code. Writte depending on your case: aa=list (list active agents to be killed). Example: --exit aa="c000@apiict00.etsii.upm.es,c014@apiict00.etsii.upm.es"')
    #DATOS DE PEDIDO:
    parser.add_argument('-oc', '--order_code', type=str, metavar='',required=False, default='No', help='Specify the number code of the order. Write between "x"')
    parser.add_argument('-sd', '--order_shdate', type=str, metavar='',required=False, default='1970-01-01', help='Specify the ship date of the order. Example: --sd "2021-11-01" ')
    parser.add_argument('-sg', '--steel_grade', type=str, metavar='', required=False, default='1', help='Number which specifies the type of steel used for coils in an order.Write between "x"')
    parser.add_argument('-at', '--average_thickness', type=float, metavar='', required=False, default='0.4',help='Specify the thickness for coils ordered')
    parser.add_argument('-wi', '--width_coils', type=int, metavar='', required=False, default='950', help='Specify the width for coils ordered')
    parser.add_argument('-nc', '--number_coils', type=int, metavar='', required=False, default='1', help='Number of coils involved in the order')
    parser.add_argument('-lc', '--list_coils', type=str, metavar='', required=False, default='No', help='List of codes of coils involved in the order.Write between "x"')
    parser.add_argument('-ll', '--list_lengths', type=str, metavar='', required=False, default='No', help='List of coil lenghts involved in the order.')
    parser.add_argument('-po', '--price_order', type=float, metavar='', required=False, default='1', help='Price given to the order')
    parser.add_argument('-so', '--string_operations', type=str, metavar='', required=False, default='No', help='Sequence of operations needed.Write between "x".Format:"BZA|TD[2]|ENT[2|3]|HO[1|2]|NWW[1|4]|VA*[9|10|11], Forma:"NWW"')
    parser.add_argument('-lp', '--list_position', type=str, metavar='', required=False, default='No',help='Previous station (operation).Write between ",".Format:CA_03,BA_01,BA_02...')
    parser.add_argument('-lstg', '--list_warehouse', type=str, metavar='', required=False, default='No',help='Coil warehouses.Write between ",".Format:K,L,K')
    parser.add_argument('-cb', '--change_budget', type=str, metavar='', required=False, default='210', help='Specify the new budget. Write between "x"')
    parser.add_argument('-na', '--name_new_budget', type=str, metavar='', required=False, default='No', help='Specify the coil of new budget. "cO202106101"')
    parser.add_argument('-F', '--parameter_F', type=int, metavar='', required=False, default=10, help='parameter_F: 10-79')
    # MANAGEMENT DATA
    parser.add_argument('-u', '--user_name', type=str, metavar='', required=False, help='User to the XMPP platform')  # JOM 10/10
    parser.add_argument('-p', '--user_passwd', type=str, metavar='', required=False, help='Passwd for the XMPP platform')  # JOM 10/10
    parser.add_argument('-lag', '--log_agnt_id', type=str, metavar='', required=False, help='User ID for the log agent')
    parser.add_argument('-bag', '--brw_agnt_id', type=str, metavar='', required=False, help='User ID for the browser agent')
    #
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = str(args.user_name)
    wait_msg_time = args.wait_msg_time
    la_started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    la_status_var = args.status
    la_search = args.search
    la_exit = args.exit
    order_code = args.order_code
    ship_date = args.order_shdate
    steel_grade = args.steel_grade
    thickness = args.average_thickness
    width_coils = args.width_coils
    num_coils = args.number_coils
    param_f   = args.parameter_F
    each_coil_price = round((args.price_order/args.number_coils),2)
    list_coils = args.list_coils
    list_lengths = args.list_lengths
    string_operations = args.string_operations
    globals.string_operations = string_operations
    list_ware = args.list_warehouse
    prev_station = args.list_position
    change_budget = args.change_budget
    name_coil = args.name_new_budget


    """Save to csv who I am"""
    la_data_df = opf.set_agent_parameters(my_name, my_full_name,0,0,0,\
                                          param_f,'','','')

    """IP"""
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
        la_jid = args.user_name
        globals.glhr_jid = la_jid
    else:
        la_jid = opf.agent_jid(my_dir, my_full_name)
    if len(args.user_passwd) > 0:
        la_passwd = args.user_passwd
    else:
        la_passwd = opf.agent_passwd(my_dir, my_full_name)
    globals.glhr_pwd = la_passwd
    globals.tosend = []
    answeryet = False
    dact  = pd.DataFrame()
    #
    la_agent = LaunchAgent(la_jid, la_passwd)
    future = la_agent.start(auto_register=True)
    future.result()
    # la_agent.b2.join()

    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while la_agent.is_alive():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            la_status_var = "off"
            la_agent.stop()
    quit_spade()
