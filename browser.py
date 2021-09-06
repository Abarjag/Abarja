from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import pandas as pd
import operative_functions as opf
import argparse
import os
import json
import socket


class BrowserAgent(Agent):
    class BRBehav(CyclicBehaviour):
        async def run(self):
            global br_status_var, my_full_name, br_started_at, stop_time, my_dir, wait_msg_time, br_coil_name_int_fab, br_int_fab, br_data_df, ip_machine
            if br_status_var == "on":
                if br_int_fab == "yes":
                    """Send msg to coil that was interrupted during fab"""
                    int_fab_msg_body = opf.br_int_fab_df(br_data_df).to_json(orient="records")
                    coil_jid = opf.agent_jid(br_coil_name_int_fab, my_dir)
                    br_coil_msg = opf.br_msg_to(int_fab_msg_body)
                    br_coil_msg.to = coil_jid
                    await self.send(br_coil_msg)
                    """inform log of event"""
                    br_msg_log_body = f'{my_full_name} send msg to {br_coil_name_int_fab} because its fab was interrupted'
                    br_msg_log_body = json.dumps(br_msg_log_body)
                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                    await self.send(br_msg_log)
                msg = await self.receive(timeout=wait_msg_time) # wait for a message for 60 seconds
                if msg:
                    sender = str(msg.sender)
                    sender_2 = sender[:-9]
                    sender = sender[:-33]
                    if sender == 'nww':
                        nww_data_df = pd.read_json(msg.body)
                        """Prepare reply"""
                        br_msg_nww = opf.msg_to_sender(msg)
                        if nww_data_df.loc[0, 'purpose'] == "request":  # If the resource requests information, browser provides it.
                            if nww_data_df.loc[0, 'request_type'] == "active users location & op_time":  # provides active users, and saves request.
                                coil_request = nww_data_df.loc[0, 'request_type']
                                """Checks for active users and their actual locations and reply"""
                                br_msg_nww_body = opf.check_active_users_loc_times(nww_data_df, my_name,
                                                                                  coil_request)  # specifies request as argument
                                if not br_msg_nww_body.empty:
                                    br_msg_nww_body_json = br_msg_nww_body.to_json(orient="records")
                                    br_msg_nww.body = br_msg_nww_body_json
                                    await self.send(br_msg_nww)
                                    """Inform log of performed request"""
                                coils = br_msg_nww_body['agent'].to_list()
                                locations = br_msg_nww_body['location'].to_list()
                                br_msg_nww_body = opf.answer_nww(br_msg_nww_body, sender_2, nww_data_df, str(coils), str(locations))
                                br_msg_nww_body = br_msg_nww_body.to_json(orient="records")
                                br_msg_log = opf.msg_to_log(br_msg_nww_body, my_dir)
                                await self.send(br_msg_log)
                            elif nww_data_df.loc[0, 'request_type'] == "coils":
                                """Checks for active coils and their actual locations and reply"""
                                coil_request = nww_data_df.loc[0, 'request_type']
                                br_msg_nww_body = opf.check_active_users_loc_times(nww_data_df, my_name,
                                                                                  coil_request)  # specifies request as argument
                                if not br_msg_nww_body.empty:
                                    br_msg_nww_body_json = br_msg_nww_body.to_json(orient="records")
                                    br_msg_nww.body = br_msg_nww_body_json
                                    await self.send(br_msg_nww)
                                    """Inform log of performed request"""
                                coils = br_msg_nww_body['agent'].to_list()
                                locations = br_msg_nww_body['location'].to_list()
                                br_msg_nww_body = opf.answer_nww(br_msg_nww_body, sender_2, nww_data_df, str(coils),
                                                               str(locations))
                                br_msg_nww_body = br_msg_nww_body.to_json(orient="records")
                                br_msg_log = opf.msg_to_log(br_msg_nww_body, my_dir)
                                await self.send(br_msg_log)
                            else:
                                """inform log"""
                                nww_id = nww_data_df.loc[0, 'id']
                                br_msg_log_body = f'{nww_id} did not set a correct type of request'
                                br_msg_log_body = opf.inform_error(br_msg_log_body)
                                br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                                await self.send(br_msg_log)
                        else:
                            """inform log"""
                            nww_id = nww_data_df.loc[0, 'id']
                            br_msg_log_body = f'{nww_id} did not set a correct purpose'
                            br_msg_log_body = opf.inform_error(br_msg_log_body)
                            br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                            await self.send(br_msg_log)
                    '''elif sender == 'c0':
                        coil_data_df = pd.read_json(msg.body)
                        """Prepare reply"""
                        br_msg_coil = opf.msg_to_sender(msg)
                        if coil_data_df.loc[0, 'purpose'] == "request":
                            if coil_data_df.loc[0, 'request_type'] == "my location":
                                coil_code = coil_data_df.loc[0, 'Code']
                                msg_to_log = opf.order_code_log(coil_code, coil_data_df, my_full_name)
                                msg_to_log_json = msg_to_log.to_json(orient="records")
                                br_loc_log = opf.msg_to_log(msg_to_log_json, my_dir)
                                await self.send(br_loc_log)
                                i = 0
                                while i < 5:
                                    i = i + 1
                                    log_to_br_msg = await self.receive(timeout=10)
                                    if log_to_br_msg:
                                        loc_df = pd.read_json(log_to_br_msg.body)
                                        msg_sender_jid = str(log_to_br_msg.sender)
                                        msg_sender_jid = msg_sender_jid[:-31]
                                        if msg_sender_jid == 'log':
                                            br_msg_coil_jid = str(msg.sender)
                                            br_msg_coil.to = br_msg_coil_jid[:-9]
                                            br_msg_coil.body = loc_df.to_json()
                                            await self.send(br_msg_coil)
                                            "Inform log"
                                            br_msg_nww_body = opf.answer_coil(loc_df, br_msg_coil_jid[:-9], msg_to_log)
                                            br_msg_nww_body = br_msg_nww_body.to_json(orient="records")
                                            br_msg_log = opf.msg_to_log(br_msg_nww_body, my_dir)
                                            await self.send(br_msg_log)
                                            break
                                    else:
                                        """inform log"""
                                        coil_id = coil_data_df.loc[0, 'id']
                                        br_msg_log_body = f'{coil_id} did not receive any msg in the last 10s'
                                        br_msg_log_body = opf.inform_error(br_msg_log_body)
                                        br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                                        await self.send(br_msg_log)
                            else:
                                """inform log"""
                                coil_id = coil_data_df.loc[0, 'id']
                                br_msg_log_body = f'{coil_id} did not set a correct type of request'
                                br_msg_log_body = opf.inform_error(br_msg_log_body)
                                br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                                await self.send(br_msg_log)
                        else:
                            """inform log"""
                            coil_id = coil_data_df.loc[0, 'id']
                            br_msg_log_body = f'{coil_id} did not set a correct purpose'
                            br_msg_log_body = opf.inform_error(br_msg_log_body)
                            br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                            await self.send(br_msg_log)'''
                else:
                    """inform log"""
                    br_msg_log_body = f'{my_name} did not receive a message in the last {wait_msg_time}s'
                    br_msg_log_body = opf.inform_error(br_msg_log_body)
                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                    await self.send(br_msg_log)
            elif br_status_var == "stand-by":  # stand-by status for BR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won      t change to stand-by.
                """inform log of status"""
                br_inform_json = opf.log_status(my_full_name, br_status_var, ip_machine)
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                br_status_var = "on"
                """inform log of status"""
                br_inform_json = opf.log_status(my_full_name, br_status_var, ip_machine)
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var, br_data_df).to_json(
                    orient="records")
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
            else:
                """inform log of status"""
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var, br_data_df).to_json(orient="records")
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                br_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})
            """Inform log """
            browser_msg_ended = opf.send_activation_finish(my_full_name, ip_machine, 'end')
            browser_msg_ended = opf.msg_to_log(browser_msg_ended, my_dir)
            await self.send(browser_msg_ended)

        async def on_start(self):
            self.counter = 1
            """Inform log """
            browser_msg_start = opf.send_activation_finish(my_full_name, ip_machine, 'start')
            browser_msg_start = opf.msg_to_log(browser_msg_start, my_dir)
            await self.send(browser_msg_start)

    async def setup(self):
        b = self.BRBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='br parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=800, help='wait_msg_time: time in seconds to wait for a msg. Purpose of system monitoring.')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent isnt asleep')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, Off')
    parser.add_argument('-if', '--interrupted_fab', type=str, metavar='', required=False, default='no', help='interrupted_fab: yes if it was stopped. We set this while system working and will tell cn:coil_number  that its fab was stopped')
    parser.add_argument('-cn', '--coil_number_interrupted_fab', type=str, metavar='', required=False, default='no', help='agent_number interrupted fab: specify which coil number fab was interrupted: 1,2,3,4.')
#
    parser.add_argument('-se','--search',type=str,metavar='',required=False,default='No',help='Search order by code. Writte depending on your case: oc (order_code),sg(steel_grade),at(average_thickness), wi(width_coils), ic(id_coil), so(string_operations),date.Example: --search oc = 987date.Example: --search oc = 987')
    parser.add_argument('-set', '--search_time', type=float, metavar='', required=False, default=0.3, help='search_time: time in seconds where agent is searching by code')
    args = parser.parse_args()
    my_dir = os.getcwd()
    agents = opf.agents_data()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    br_started_at = datetime.datetime.now()
    br_status_var = args.status
    br_int_fab = args.interrupted_fab
    br_search = args.search
    coil_agent_name = "coil"
    coil_agent_number = args.coil_number_interrupted_fab
    br_coil_name_int_fab = opf.my_full_name(coil_agent_name, coil_agent_number)
    searching_time = datetime.datetime.now() + datetime.timedelta(seconds=args.search_time)
    """Save to csv who I am"""
    br_data_df = opf.set_agent_parameters(my_dir, my_name, my_full_name)
    #opf.br_create_register(my_dir, my_full_name)  # register to store entrance and exit
    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]

    """XMPP info"""
    br_jid = opf.agent_jid(my_dir, my_full_name)
    br_passwd = opf.agent_passwd(my_dir, my_full_name)
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
