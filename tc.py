from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import sys
import pandas as pd
import logging
import argparse
import operative_functions as opf
import os


class TransportAgent(Agent):
    class TRBehav(CyclicBehaviour):
        async def run(self):
            global tr_status_var, my_full_name, tr_status_started_at, stop_time, my_dir, wait_msg_time
            """inform log of status"""
            tr_activation_json = opf.activation_df(my_full_name, tr_status_started_at)
            tr_msg_log = opf.msg_to_log(tr_activation_json, my_dir)
            await self.send(tr_msg_log)
            if tr_status_var == "on":
                """inform log of status"""
                tr_inform_json = opf.inform_log_df(my_full_name, 'tc',tr_status_started_at, tr_status_var).to_json()
                tr_msg_log = opf.msg_to_log(tr_inform_json, my_dir)
                await self.send(tr_msg_log)
                nww_tr_msg = await self.receive(timeout=wait_msg_time)  # wait for a message for 5 seconds
                if nww_tr_msg:
                    nww_data_df = pd.read_json(nww_tr_msg.body)
                    if nww_data_df.loc[0, 'action'] == "pre-book":
                        """Prepare reply to nww of availability"""
                        tr_msg_nww = opf.msg_to_sender(nww_tr_msg)
                        """Read when tr is needed"""
                        slot_range = opf.slot_to_minutes(nww_data_df)
                        tr_msg_nww.body = opf.tr_check_availability(my_dir, my_full_name, slot_range)  # Returns message of availability
                        await self.send(tr_msg_nww)
                        if tr_msg_nww.body == "positive":  # if negative, nothing, nww will send a list of the asked tr and the booked one to log. That way we can trace if tr_x was available or not.
                            """Append pre-booking"""
                            tr_msg_log_body = opf.tr_append_booking(my_dir, my_full_name, nww_data_df, slot_range)  # Returns booking info
                            """inform log"""
                            tr_msg_log = opf.msg_to_log(tr_msg_log_body, my_dir)
                            await self.send(tr_msg_log)
                    elif nww_data_df.loc[0, 'action'] == "booked":
                        """Append booking"""
                        slot_range = opf.slot_to_minutes(nww_data_df)
                        tr_msg_log_body = opf.tr_append_booking(my_dir, my_full_name, nww_data_df, slot_range)  # Returns booking info
                        """inform log"""
                        tr_msg_log = opf.msg_to_log(tr_msg_log_body, my_dir)
                        await self.send(tr_msg_log)
                    else:
                        """inform log"""
                        nww_id = nww_data_df.loc[0, 'id']
                        tr_msg_log_body = f'{nww_id} did not set a correct action'
                        tr_msg_log = opf.msg_to_log(tr_msg_log_body, my_dir)
                        await self.send(tr_msg_log)
                else:
                    """inform log"""
                    tr_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s'
                    tr_msg_log = opf.msg_to_log(tr_msg_log_body, my_dir)
                    await self.send(tr_msg_log)
            elif tr_status_var == "stand-by":  # stand-by status for TR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won´t change to stand-by.
                """inform log of status"""
                tr_inform_json = opf.inform_log_df(my_full_name,'tc', tr_status_started_at, tr_status_var).to_json()
                tr_msg_log = opf.msg_to_log(tr_inform_json, my_dir)
                await self.send(tr_msg_log)
                # We could introduce here a condition to be met to change to "on"
                # now it just changes directly to auction
                """inform log of status"""
                tr_status_var = "on"
                tr_inform_json = opf.inform_log_df(my_full_name,'tc', tr_status_started_at, tr_status_var).to_json()
                tr_msg_log = opf.msg_to_log(tr_inform_json, my_dir)
                await self.send(tr_msg_log)
            else:
                """inform log of status"""
                tr_inform_json = opf.inform_log_df(my_full_name,'tc', tr_status_started_at, tr_status_var).to_json()
                tr_msg_log = opf.msg_to_log(tr_inform_json, my_dir)
                await self.send(tr_msg_log)
                tr_status_var = "stand-by"


        async def on_end(self):
            print({self.counter})

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        b = self.TRBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='tc parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, Off')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    tr_status_started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tr_status_refresh = datetime.datetime.now() + datetime.timedelta(seconds=5)
    tr_status_var = args.status
    """Save to csv who I am"""
    opf.set_agent_parameters(my_dir, my_name, my_full_name,0,0,0,'','','')
    opf.tr_create_booking_register(my_dir, my_full_name)  # register to store bookings
    """XMPP info"""
    tr_jid = opf.agent_jid(my_dir, my_full_name)
    tr_passwd = opf.agent_passwd(my_dir, my_full_name)
    tr_agent = TransportAgent(tr_jid, tr_passwd)
    future = tr_agent.start(auto_register=True)
    future.result()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        tr_status_var = "off"
        tr_agent.stop()
        quit_spade()
