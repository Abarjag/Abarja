from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import sys
import pandas as pd
import operative_functions as opf
import argparse
import os


class BrowserAgent(Agent):
    class BRBehav(CyclicBehaviour):
        async def run(self):
            global br_status_var, my_full_name, br_started_at, stop_time, my_dir, wait_msg_time, br_coil_name_int_fab, br_int_fab, br_data_df
            """inform log of status"""
            br_activation_json = opf.activation_df(my_full_name, br_started_at)
            br_msg_log = opf.msg_to_log(br_activation_json, my_dir)
            await self.send(br_msg_log)
            if br_status_var == "on":
                """inform log of status"""
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var).to_json()
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                if br_int_fab == "yes":
                    """Send msg to coil that was interrupted during fab"""
                    int_fab_msg_body = opf.br_int_fab_df(br_data_df).to_json()
                    coil_jid = opf.get_agent_jid(br_coil_name_int_fab, my_dir)
                    br_coil_msg = opf.br_msg_to(int_fab_msg_body)
                    br_coil_msg.to = coil_jid
                    await self.send(br_coil_msg)
                    """inform log of event"""
                    br_msg_log_body = f'{my_full_name} send msg to {br_coil_name_int_fab} because its fab was interrupted'
                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                    await self.send(br_msg_log)
                    print(br_msg_log_body)
                nww_br_msg = await self.receive(timeout=wait_msg_time)  # wait for a message for 5 seconds
                if nww_br_msg:
                    print(f'nww_br_msg: {nww_br_msg.body}')
                    nww_data_df = pd.read_json(nww_br_msg.body)
                    """Prepare reply"""
                    br_msg_nww = opf.msg_to_sender(nww_br_msg)
                    if nww_data_df.loc[0, 'purpose'] == "request":  # If the resource requests information, browser provides it.
                        if nww_data_df.loc[0, 'request_type'] == "active users location & op_time":  # provides active users, and saves request.
                            """Checks for active users and their actual locations and reply"""
                            nww_name = nww_data_df.loc[0, 'agent_type']
                            br_msg_nww_body = opf.check_active_users_loc_times(nww_name)  # provides agent_id as argument
                            br_msg_nww.body = br_msg_nww_body
                            print(f'br_msg_nww active users: {br_msg_nww.body}')
                            await self.send(br_msg_nww)
                            """Inform log of performed request"""
                            br_msg_log = opf.msg_to_log(br_msg_nww_body, my_dir)
                            await self.send(br_msg_log)
                        elif nww_data_df.loc[0, 'request_type'] == "coils":
                            """Checks for active coils and their actual locations and reply"""
                            coil_request = nww_data_df.loc[0, 'request_type']
                            br_msg_nww_body = opf.check_active_users_loc_times(my_name, coil_request)  # specifies request as argument
                            br_msg_nww.body = br_msg_nww_body
                            print(f'br_msg_ca coils: {br_msg_nww.body}')
                            await self.send(br_msg_nww)
                            """Inform log of performed request"""
                            br_msg_log = opf.msg_to_log(br_msg_nww_body, my_dir)
                            await self.send(br_msg_log)
                        else:
                            """inform log"""
                            nww_id = nww_data_df.loc[0, 'id']
                            br_msg_log_body = f'{nww_id} did not set a correct type of request'
                            br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                            await self.send(br_msg_log)
                    else:
                        """inform log"""
                        nww_id = nww_data_df.loc[0, 'id']
                        br_msg_log_body = f'{nww_id} did not set a correct purpose'
                        br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                        await self.send(br_msg_log)
                else:
                    """inform log"""
                    br_msg_log_body = f'{my_name} did not receive a message in the last {wait_msg_time}s'
                    br_msg_log = opf.msg_to_log(br_msg_log_body, my_dir)
                    await self.send(br_msg_log)
            elif br_status_var == "stand-by":  # stand-by status for BR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won´t change to stand-by.
                """inform log of status"""
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var).to_json()
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                # We could introduce here a condition to be met to change to "on"
                # now it just changes directly to auction
                """inform log of status"""
                br_status_var = "on"
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var).to_json()
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
            else:
                """inform log of status"""
                br_inform_json = opf.inform_log_df(my_full_name, br_started_at, br_status_var).to_json()
                br_msg_log = opf.msg_to_log(br_inform_json, my_dir)
                await self.send(br_msg_log)
                br_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        b = self.BRBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=60, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, Off')
    parser.add_argument('-if', '--interrupted_fab', type=str, metavar='', required=False, default='no', help='interrupted_fab: yes if it was stopped. We set this while system working and will tell cn:coil_number that its fab was stopped')
    parser.add_argument('-cn', '--coil_number_interrupted_fab', type=str, metavar='', required=False, default='no', help='agent_number interrupted fab: specify which coil number fab was interrupted: 1,2,3,4..')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    br_started_at = datetime.datetime.now().time()
    br_status_var = args.status
    br_int_fab = args.interrupted_fab
    coil_agent_name = "coil"
    coil_agent_number = args.coil_number_interrupted_fab
    br_coil_name_int_fab = opf.my_full_name(coil_agent_name, coil_agent_number)
    """Save to csv who I am"""
    opf.set_agent_parameters(my_dir, my_name, my_full_name)
    br_data_df = pd.read_csv(f'{my_full_name}.csv', header=0, delimiter=",", engine='python')
    #opf.br_create_register(my_dir, my_full_name)  # register to store entrance and exit
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
