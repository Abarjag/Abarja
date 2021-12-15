import time, datetime, syslog, pdb
import sys, os, socket, json, random
import argparse, globals
import pandas as pd
import operative_functions as opf
from spade import quit_spade
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message

# Global variable supporting the identity of the log agent
glog_jid = ''

class BrowserAgent(Agent):
    class BRBehav(CyclicBehaviour):
        async def run(self):
            global br_status_var, my_full_name, br_started_at, stop_time, my_dir, \
                   wait_msg_time, br_coil_name_int_fab, br_int_fab, br_data_df, \
                   ip_machine, glog_jid

            if br_status_var == "on":
                """inform log of status"""
                if hasattr(locals,'old_br_st_var'):
                    if (old_br_st_var != br_status_var):
                        br_inform_json = opf.inform_log_df(my_full_name,\
                                 'browser', br_started_at, br_status_var\
                                 ).to_json()
                        br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                        await self.send(br_msg_log)
                else:
                    old_status_var = ''
                old_br_st_var = "on"
                if br_int_fab == "yes":
                    """Send msg to coil that was interrupted during fab"""
                    int_fab_msg_body = opf.br_int_fab_df(br_data_df).to_json()
                    coil_jid = opf.get_agent_jid(br_coil_name_int_fab, my_dir)
                    br_coil_msg = opf.br_msg_to(int_fab_msg_body)
                    br_coil_msg.to = coil_jid
                    await self.send(br_coil_msg)
                    """inform log of event"""
                    br_msg_log_body = f'{my_full_name} send msg to {br_coil_name_int_fab}'
                    br_msg_log_body = br_msg_lg_body + ' because its fab was interrupted '
                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                    await self.send(br_msg_log)
                    # print(br_msg_log_body)

                """ Main loop, waiting for messages to be handled out """
                msg = await self.receive(timeout=wait_msg_time)
                if msg:
                    agent_df = pd.read_json(msg.body)
                    msg_sender_jid0 = str(msg.sender)
                    msg_sender_jid = msg_sender_jid0.split('/')[0]
                    if 'log' in msg_sender_jid:
                        contact_list = []
                        if agent_df.loc[0,'purpose'] == 'answer_clist':
                            id_agnt = agent_df.loc[0,'seq']
                            contact_list = json.loads(agent_df.loc[0, 'msg'])
                            to_agnt, to_id = self.ret_agnt(id_agnt)
                            cl_agent= opf.rq_list(my_full_name, contact_list, \
                                         to_agnt,'contact_list',to_id)
                            cl_ans  = opf.contact_list_json(cl_agent,to_agnt)
                            await self.send(cl_ans)
                            glist = self.del_agnt(to_id,globals.tosend)
                            globals.tosend = glist

                    elif agent_df.loc[0, 'purpose'] == "delete":
                        # message to remove coils of the active coils
                        delcl = pd.read_json(agent_df.loc[0, 'msg'])
                        dact  = globals.dact
                        globals.dact = dact[dact.id != delcl.loc[0,'id']]

                    elif agent_df.loc[0, 'purpose'] == "create":
                        # Adding a new coil to the list of active coils
                        newcl  = pd.read_json(agent_df.loc[0, 'msg'])
                        dact   = globals.dact
                        if dact.shape[0] > 0:
                            dact   = dact.loc[dact['id'] != newcl.loc[0,'id'],:]
                        globals.dact = pd.concat([dact,newcl],ignore_index = True)

                    elif agent_df.loc[0, 'purpose'] == "update":
                        # Replacing a new coil to the list of active coils
                        dact   = globals.dact
                        updcl  = pd.read_json(agent_df.loc[0, 'msg'])
                        updcl  = updcl.rename(columns={'path':'ph','oname':'orden',\
                                    'sgrade':'sg','budget':'bdg','name':'code'})
                        if dact.shape[0] > 0:
                            dact = dact[dact.id != updcl.loc[0,'id']]
                        globals.dact = pd.concat([dact,updcl],ignore_index=True)

                    elif agent_df.loc[0, 'purpose'] == "getlist":
                        ag_rq = msg_sender_jid
                        id_ag = agent_df.loc[0,'seq']
                        dact  = globals.dact
                        lagnts= dact.to_json(orient="records")
                        inform_srch_log= opf.msg_to_log(lagnts, my_dir)
                        await self.send(inform_srch_log)
                        cl_ag = opf.rq_list(my_full_name, dact, \
                                     msg_sender_jid,'getlist',id_ag)
                        cnt_lst = opf.contact_list_json(cl_ag,msg_sender_jid)
                        await self.send(cnt_lst)

                    elif agent_df.loc[0, 'purpose'] == "contact_list":
                        id_org = agent_df.loc[0,'seq']
                        id_new = int(random.random()*10000)
                        r = 'Request contact list'
                        rq_clist = opf.rq_list(my_full_name,r,globals.glog_jid,\
                                    'contact_list',id_new)
                        rq_clist_json = opf.contact_list_json(rq_clist,'log')
                        globals.tosend.append({'idorg':id_org,'agnt_org': \
                            msg_sender_jid, 'idact':id_new,'agnt_end': \
                            globals.glog_jid})
                        await self.send(rq_clist_json)


                    elif agent_df.loc[0, 'purpose'] == "search":
                        msg_search = agent_df.loc[0, 'msg']
                        single = msg_search.split(':')
                        search = single[1]
                        if search == 'COIL_AGENT_METADATA' and single[0] == 'Search':
                            coil = single[2]
                            #
                            # request contact list to log
                            r = 'Request contact list'
                            rq_clist = opf.rq_list(my_full_name, r, \
                                              globals.glog_jid,'contact_list')
                            rql = opf.contact_list_json(rq_clist,'log', 1)
                            await self.send(rql)
                            msg_cl = await self.receive(timeout=wait_msg_time)
                            contact_list = []
                            if msg_cl:
                                cl_df = pd.read_json(msg_cl.body)
                                contact_list = json.loads(cl_df.loc[0, 'msg'])
                            #
                            msg  = '{"purpose":"request","status":"alive"}'
                            s_rg_json = '[{"User name":"'+coil+'",\
                                        "msg":"Not found","Code":"","From":"",\
                                        "Location":"","Capacity":"",\
                                        "purpose":"inform error"}]'
                            if coil in contact_list:
                                rqst = opf.msg_to_agnt(msg,coil)
                                await self.send(rqst)
                                msg_aa = await self.receive(timeout=\
                                         wait_msg_time)
                                if msg_aa:
                                    dat = json.loads(msg_aa.body)
                                    s_rg_json  = '[{"User name":"'+coil+\
                                          '","msg":"alive","Code":"' +\
                                          dat.loc[0,'msg']+'","From":"'+\
                                          dat.loc[0,'oname']+'","Location":"'+\
                                          dat.loc[0,'loc']+'","Capacity":"'+\
                                          dat.loc[0,'budget']+'","purpose":"'+\
                                          'inform"}]'
                            inform_srch_log= opf.msg_to_log(s_rg_json, my_dir)
                            await self.send(inform_srch_log)
                            inform_coil = opf.contact_list_json(s_rg_json, ag_rq, my_dir)
                            await self.send(inform_coil)

                    else:
                        if 'CA' in msg_sender_jid.upper():
                            print(f'ca_br_msg: {msg.body}')
                            ca_data_df = pd.read_json(msg.body)
                            """Prepare reply"""
                            br_msg_ca = opf.msg_to_sender(msg)
                            if ca_data_df.loc[0, 'purpose'] == "request":  # If the resource requests information, browser provides it.
                                if ca_data_df.loc[0, 'request_type'] == "active users location & op_time":  # provides active users, and saves request.
                                    """Checks for active users and their actual locations and reply"""
                                    ca_name = ca_data_df.loc[0, 'agent_type']
                                    br_msg_ca_body = opf.check_active_users_loc_times(ca_name)  # provides agent_id as argument
                                    br_msg_ca.body = br_msg_ca_body
                                    print(f'br_msg_ca active users: {br_msg_ca.body}')
                                    await self.send(br_msg_ca)
                                    """Inform log of performed request"""
                                    br_msg_log = opf.msg_to_log(br_msg_ca_body, my_dir)
                                    await self.send(br_msg_log)
                                elif ca_data_df.loc[0, 'request_type'] == "coils":
                                    """Checks for active coils and their actual locations and reply"""
                                    coil_request = ca_data_df.loc[0, 'request_type']
                                    br_msg_ca_body = opf.check_active_users_loc_times(my_name,coil_request)  # specifies request as argument
                                    br_msg_ca.body = br_msg_ca_body
                                    print(f'br_msg_ca coils: {br_msg_ca.body}')
                                    await self.send(br_msg_ca)
                                    """Inform log of performed request"""
                                    br_msg_log = opf.msg_to_log(br_msg_ca_body, my_dir)
                                    await self.send(br_msg_log)
                                else:
                                    """inform log"""
                                    ca_id = ca_data_df.loc[0, 'id']
                                    br_msg_log_body = f'{ca_id} did not set a correct type of request'
                                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                                    await self.send(br_msg_log)
                            else:
                                """inform log"""
                                ca_id = ca_data_df.loc[0, 'id']
                                br_msg_log_body = f'{ca_id} did not set a correct purpose'
                                br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                                await self.send(br_msg_log)
                else:
                    """inform log"""
                    if hasattr(locals,'old_br_st_var'):
                        if (old_br_st_var != br_status_var):
                            br_msg_log_body = f'{my_name} did not receive a message in the last {wait_msg_time}s'
                            br_msg_log_body = opf.inform_error(br_msg_log_body)
                            br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                            await self.send(br_msg_log)
                    else:
                        old_status_var = ''
                    old_br_st_var = br_status_var
            elif br_status_var == "stand-by":  # stand-by status for BR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won      t change to stand-by.
                """inform log of status"""
                if hasattr(locals,'old_br_st_var'):
                    if (old_br_st_var != br_status_var):
                        br_inform_json = opf.log_status(my_full_name, \
                                         br_status_var, ip_machine)
                        br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                        await self.send(br_msg_log)
                else:
                    old_status_var = ''
                old_br_st_var = "stand-by"

                """inform log of status"""
                br_status_var = "on"
                br_inform_json = opf.log_status(my_full_name, br_status_var, \
                        ip_machine)
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                br_inform_json = opf.inform_log_df(my_full_name, 'browser', \
                        br_started_at, br_status_var, br_data_df).to_json( \
                        orient="records")
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
            else:
                """inform log of status"""
                if hasattr(locals,'old_br_st_var'):
                    if (old_br_st_var != br_status_var):
                        br_inform_json = opf.inform_log_df(my_full_name, 'browser',\
                                br_started_at, br_status_var).to_json()
                        br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                        await self.send(br_msg_log)
                old_br_st_var = br_status_var

        async def on_end(self):
            """Inform log """
            browser_msg_ended = opf.send_activation_finish(my_full_name, ip_machine, 'end')
            browser_msg_ended = opf.msg_to_log(browser_msg_ended, my_dir)
            await self.send(browser_msg_ended)
            await self.presence.unsubscribe(globals.gbrw_jid)
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1
            """Inform log """
            browser_msg_start = opf.send_activation_finish(my_full_name, ip_machine, 'start')
            browser_msg_start = opf.msg_to_log(browser_msg_start, my_dir)
            await self.send(browser_msg_start)

        async def on_subscribe(self, jid):
            print("[{}] Agent {} asked for subscription. Let's aprove it.".format(self.agent.name, jid.split("@")[0]))
            self.presence.approve(jid)
            self.presence.subscribe(jid)

        async def on_unsubscribe(self, jid):
            print("[{}] Agent {} asked for UNsubscription. Let's aprove it.".format(self.agent.name, jid.split("@")[0]))
            self.presence.unsubscribe(jid)

        def ret_agnt(self,id_agnt):
            for idct in globals.tosend:
                if idct['idact'] == id_agnt:
                    return([idct['agnt_org'], idct['idorg']])

        def del_agnt(self,id_agnt,glist):
            for inum in range(len(glist)):
                idct = glist[inum]
                if idct['idorg'] == id_agnt:
                    glist.pop(inum)
            return(glist)

    async def setup(self):
        b = self.BRBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='br parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1,
                        help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=60,
                        help='wait_msg_time: time in seconds to wait for a msg. Purpose of system monitoring.')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600,
                        help='stop_time: time in seconds where agent isnt asleep')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='on',
                        help='status_var: on, stand-by, Off')
    parser.add_argument('-if', '--interrupted_fab', type=str, metavar='', required=False, default='no',
                        help='interrupted_fab: yes if it was stopped. We set this while system working and will tell cn:coil_number  that its fab was stopped')
    parser.add_argument('-cn', '--coil_number_interrupted_fab', type=str, metavar='', required=False, default='no',
                        help='agent_number interrupted fab: specify which coil number fab was interrupted: 1,2,3,4.')
    #
    parser.add_argument('-se', '--search', type=str, metavar='', required=False, default='No',
                        help='Search order by code. Writte depending on your case: oc (order_code),sg(steel_grade),at(average_thickness), wi(width_coils), ic(id_coil), so(string_operations),date.Example: --search oc = 987, date.Example: --search oc = 987')
    parser.add_argument('-set', '--search_time', type=float, metavar='', required=False, default=0.3,
                        help='search_time: time in seconds where agent is searching by code')
    parser.add_argument('-do', '--delete', type=str, metavar='', required=False, default='No',
                        help='Delete order in register given a code to filter')
    parser.add_argument('-u', '--user_name', type=str, metavar='', required=False, default='No',
                        help='User to the XMPP platform')  # JOM 10/10
    parser.add_argument('-p', '--user_passwd', type=str, metavar='', required=False, default='No',
                        help='Passwd for the XMPP platform')  # JOM 10/10
    parser.add_argument('-lag', '--log_agnt_id', type=str, metavar='', required=False, help='User ID for the log agent')
    parser.add_argument('-bag', '--brw_agnt_id', type=str, metavar='', required=False, help='User ID for the browser agent')
    parser.add_argument('-lhg', '--lhr_agnt_id', type=str, metavar='', required=False, help='User ID for the launcher agent')
    args = parser.parse_args()
    my_dir = os.getcwd()
    agents = opf.agents_data()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = args.user_name
    wait_msg_time = args.wait_msg_time
    br_started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    br_status_var = args.status
    br_int_fab = args.interrupted_fab
    br_search = args.search
    coil_agent_name = "coil"
    br_delete = args.delete
    coil_agent_number = args.coil_number_interrupted_fab
    br_coil_name_int_fab = opf.my_full_name(coil_agent_name, coil_agent_number)
    searching_time = datetime.datetime.now() + datetime.timedelta(seconds=args.search_time)

    """Save to csv who I am"""
    br_data_df = opf.set_agent_parameters(my_name, my_full_name,0,0,0,49,'','','')

    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]
    globals.IP = ip_machine
    globals.dact = pd.DataFrame()

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
        br_jid = args.user_name
        globals.gbrw_jid = br_jid
    else:
        br_jid = opf.agent_jid(my_dir, my_full_name)
    if len(args.user_passwd) > 0:
        br_passwd = args.user_passwd
    else:
        br_passwd = opf.agent_passwd(my_dir, my_full_name)
    #
    # Ask to reassess the agent list of active coils (every 10 min)
    globals.tosend= []
    globals.dact = pd.DataFrame()
    acttime = datetime.datetime.now() - datetime.timedelta(seconds=1000)
    br_agent = BrowserAgent(br_jid, br_passwd)
    future = br_agent.start(auto_register=True)
    future.result()

    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        br_status_var = "off"
        br_agent.stop()
        quit_spade()
