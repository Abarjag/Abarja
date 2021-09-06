from spade import quit_spade
import time
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


class CoilAgent(Agent):
    class CoilBehav(PeriodicBehaviour):
        async def run(self):
            global my_full_name, my_dir, wait_msg_time, coil_status_var, coil_started_at, stop_time, refresh_time, coil_agent, coil_data_df, bid_register_df, nww_coil_msg_sender, not_entered_auctions
            """inform log of status"""
            coil_activation_json = opf.activation_df(my_full_name, coil_started_at)
            coil_msg_log = opf.msg_to_log(coil_activation_json, my_dir)
            await self.send(coil_msg_log)
            if coil_status_var == "auction":
                """inform log of status"""
                to_do = "search-auction"
                coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var, to_do).to_json()
                coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                this_time = datetime.datetime.now()
                print(coil_status_var)
                # it will wait here for nww's that are auctionable.
                nww_coil_msg = await self.receive(timeout=wait_msg_time)
                print(nww_coil_msg)
                if nww_coil_msg:
                    nww_coil_msg_sender = nww_coil_msg.sender
                    if nww_coil_msg_sender == "launch": #in case the msg was sent by Launcher
                        la_coil_msg_df = pd.read_json(va_coil_msg.body)
                        coil_data_df.loc[0, 'budget'] = la_coil_msg_df.loc[0, 'budget']
                    else:
                        """Evaluate if resource conditions are acceptable to enter auction"""
                        nww_coil_msg_df=pd.read_json(nww_coil_msg.body)
                        coil_data_df.at[0,'F_group']=opf.F_groups(coil_data.at[0,'parameter_F'], nww_coil_msg_df.at[0,'id'] )
                        coil_enter_auction_rating = opf.coil_enter_auction_rating(nww_coil_msg_df, coil_data_df, not_entered_auctions)
                        print(f'nww_coil_msg_df: {nww_coil_msg_df.to_string()}')
                        if coil_enter_auction_rating > 0:
                            #auction_level = nww_coil_msg_df.loc[0, 'auction_level']
                            """Create initial Bid"""
                            coil_bid = opf.coil_bid(nww_coil_msg_df, coil_data_df, coil_status_var, coil_enter_auction_rating)
                            """Send bid to nww"""
                            coil_nww_msg = opf.msg_to_sender(nww_coil_msg)
                            coil_data_df.loc[0, 'bid'] = coil_bid
                            coil_data_df.loc[0, 'bid_status'] = 'counterbid'
                            coil_nww_msg.body = coil_data_df.to_json()
                            await self.send(coil_nww_msg)
                            """Store initial Bid"""
                            bid_level = 'initial'
                            bid_register_df = opf.append_bid(coil_bid, bid_register_df, my_name, my_full_name, nww_coil_msg_df, bid_level)
                            """inform log of status"""
                            coil_status_var = "auction"  # moves now to auction status
                            coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                            coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                            await self.send(coil_msg_log)
                            """inform log of bid_register"""
                            coil_inform_json = bid_register_df.to_json()
                            coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                            await self.send(coil_msg_log)
                            """Receive request to counterbid or acceptedbid"""
                            nww_coil_msg2 = await self.receive(timeout=wait_msg_time)
                            # add counter to come back to stand-by if auction does not come to and end.
                            if nww_coil_msg2:
                                nww_coil_msg_df_2 = pd.read_json(nww_coil_msg2.body)
                                if nww_coil_msg2.sender == nww_coil_msg_sender:  # checks if communication comes from last sender
                                    a = nww_coil_msg_df_2.at[0, 'bid_status']
                                    print(f'{a}')
                                    print(nww_coil_msg_df_2)
                                    if nww_coil_msg_df_2.at[0, 'bid_status'] == 'acceptedbid':
                                        """Store accepted Bid from nww agent"""
                                        bid_level = 'acceptedbid'
                                        bid_register_df = opf.append_bid(coil_bid, bid_register_df, my_name, my_full_name, nww_coil_msg_df_2, bid_level)
                                        """inform log of bid_register"""
                                        coil_inform_json = bid_register_df.to_json()
                                        coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                        await self.send(coil_msg_log)
                                        """Confirm or deny assignation"""
                                        best_auction_agent_full_name = opf.compare_auctions(bid_register_df)
                                        """Store accepted Bid from coil agent"""
                                        bid_level = 'confirm'
                                        accepted_jid = opf.get_agent_jid(best_auction_agent_full_name)
                                        bid_register_df = opf.append_bid(coil_bid, bid_register_df, my_name, my_full_name, nww_coil_msg_df_2, bid_level, best_auction_agent_full_name)
                                        print(f'accepted jid: {accepted_jid}')
                                        print(f'nww_coil_msg_sender jid: {nww_coil_msg_sender}')
                                        nww_coil_msg_sender_f = str(nww_coil_msg_sender)[:-9]
                                        print(f'nww_coil_msg_sender jid: {nww_coil_msg_sender_f}')
                                        accepted_jid = str(accepted_jid)
                                        if accepted_jid == nww_coil_msg_sender_f:
                                            # confirm assignation. Else nothing
                                            coil_nww_msg = opf.msg_to_sender(nww_coil_msg)
                                            coil_data_df.loc[0, 'bid'] = coil_bid
                                            coil_data_df.loc[0, 'bid_status'] = 'acceptedbid'
                                            coil_nww_msg.body = coil_data_df.to_json()
                                            await self.send(coil_nww_msg)
                                            """inform log of auction won"""
                                            nww_id = nww_coil_msg_df_2.loc[0, 'id']
                                            this_time = datetime.datetime.now()
                                            coil_msg_log_body = f'{my_full_name} won auction to process in {nww_id} at {this_time}'
                                            coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                            await self.send(coil_msg_log)
                                            print(coil_msg_log_body)
                                            """inform log status change"""
                                            coil_status_var = "sleep"  # changes to sleep
                                            coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                                            coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                            await self.send(coil_msg_log)
                                            """inform log of bid_register"""
                                            coil_inform_json = bid_register_df.to_json()
                                            coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                            await self.send(coil_msg_log)
                                        else:
                                            """inform log of issue"""
                                            nww_id = nww_coil_msg_df_2.loc[0, 'id']
                                            coil_msg_log_body = f'{my_full_name} did not accept to process in {nww_id} in final acceptance'
                                            coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                            await self.send(coil_msg_log)
                                            print(coil_msg_log_body)
                                    elif nww_coil_msg_df_2.at[0, 'bid_status'] == 'extrabid':
                                        """Create extra Bid"""
                                        coil_extrabid = opf.coil_bid(nww_coil_msg_df_2, coil_data_df, coil_status_var, coil_enter_auction_rating)  # will give 10% extra budget
                                        """Store extra Bid"""
                                        bid_level = 'extrabid'
                                        bid_register_df = opf.append_bid(coil_extrabid, bid_register_df, my_name, my_full_name, nww_coil_msg_df_2, bid_level)
                                        """Send bid to nww"""
                                        coil_nww_msg = opf.msg_to_sender(nww_coil_msg2)
                                        coil_data_df.loc[0, 'extrabid'] = coil_extrabid
                                        coil_data_df.loc[0, 'CounterBid'] = 'counterbid'
                                        coil_nww_msg.body = coil_data_df.to_json()
                                        await self.send(coil_nww_msg)
                                        nww_coil_msg_sender = nww_coil_msg2.sender
                                        """inform log of bid_register"""
                                        coil_inform_json = bid_register_df.to_json()
                                        coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                        await self.send(coil_msg_log)
                                        """Wait to receive acceptance"""
                                        nww_coil_msg3 = await self.receive(timeout=wait_msg_time)
                                        # add counter to come back to stand-by if auction does not come to and end.
                                        if nww_coil_msg3:
                                            nww_coil_msg_df_3 = pd.read_json(nww_coil_msg3.body)
                                            if nww_coil_msg3.sender == nww_coil_msg_sender:  # checks if communication comes from last sender
                                                a = nww_coil_msg_df_3.at[0, 'bid_status']
                                                print(f'{a}')
                                                print(nww_coil_msg_df_3)
                                                if nww_coil_msg_df_3.at[0, 'bid_status'] == 'acceptedbid' and nww_coil_msg_df_3.at[0, 'auction_level'] == 3:
                                                    """Store accepted Bid from nww agent"""
                                                    bid_level = 'acceptedbid'
                                                    bid_register_df = opf.append_bid(coil_extrabid, bid_register_df, my_name, my_full_name, nww_coil_msg_df_3, bid_level)
                                                    """inform log of bid_register"""
                                                    coil_inform_json = bid_register_df.to_json()
                                                    coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                                    await self.send(coil_msg_log)
                                                    """Confirm or deny assignation"""
                                                    best_auction_agent_full_name = opf.compare_auctions(bid_register_df)
                                                    """Store accepted Bid from coil agent"""
                                                    bid_level = 'confirm'
                                                    accepted_jid = opf.get_agent_jid(best_auction_agent_full_name)
                                                    bid_register_df = opf.append_bid(coil_extrabid, bid_register_df, my_name, my_full_name, nww_coil_msg_df_3, bid_level, best_auction_agent_full_name)
                                                    print(f'accepted jid: {accepted_jid}')
                                                    print(f'nww_coil_msg_sender jid: {nww_coil_msg_sender}')
                                                    nww_coil_msg_sender_f = str(nww_coil_msg_sender)[:-9]
                                                    print(f'nww_coil_msg_sender jid: {nww_coil_msg_sender_f}')
                                                    accepted_jid = str(accepted_jid)
                                                    if accepted_jid == nww_coil_msg_sender_f:
                                                        # confirm assignation. Else nothing
                                                        coil_nww_msg = opf.msg_to_sender(nww_coil_msg)
                                                        coil_data_df.loc[0, 'bid_status'] = 'acceptedbid'
                                                        coil_nww_msg.body = coil_data_df.to_json()
                                                        await self.send(coil_nww_msg)
                                                        """inform log of auction won"""
                                                        nww_id = nww_coil_msg_df_3.loc[0, 'id']
                                                        this_time = datetime.datetime.now()
                                                        coil_msg_log_body = f'{my_full_name} won auction to process in {nww_id} at {this_time}'
                                                        coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                        print(coil_msg_log_body)
                                                        """inform log status change"""
                                                        coil_status_var = "sleep"  # changes to sleep
                                                        coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                                                        coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                                        await self.send(coil_msg_log)
                                                        """inform log of bid_register"""
                                                        coil_inform_json = bid_register_df.to_json()
                                                        coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                                                        await self.send(coil_msg_log)
                                                    else:
                                                        """inform log of issue"""
                                                        nww_id = nww_coil_msg_df_3.loc[0, 'id']
                                                        coil_msg_log_body = f'{my_full_name} did not accept to process in {nww_id} in final acceptance'
                                                        coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                        print(coil_msg_log_body)
                                                else:
                                                    """inform log of issue"""
                                                    nww_id = nww_coil_msg_df_3.loc[0, 'id']
                                                    nww_bid_status = nww_coil_msg_df_3.at[0, 'bid_status']
                                                    nww_auction_level = nww_coil_msg_df_3.at[0, 'auction_level']
                                                    coil_msg_log_body = f'{my_full_name} received wrong message from {nww_id} in final acceptance. nww_auction_level: {nww_auction_level}!= 3 or nww_bid_status: {nww_bid_status} != accepted'
                                                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                                    await self.send(coil_msg_log)
                                                    print(coil_msg_log_body)
                                            else:
                                                """inform log of issue"""
                                                coil_msg_log_body = f'incorrect sender'
                                                coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                                await self.send(coil_msg_log)
                                                print(coil_msg_log_body)
                                        else:
                                            """inform log"""
                                            coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                            coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                            await self.send(coil_msg_log)
                                            print(coil_msg_log_body)
                                    else:
                                        """inform log of issue"""
                                        nww_id = nww_coil_msg_df_2.loc[0, 'id']
                                        nww_status = nww_coil_msg_df_2.loc[0, 'bid_status']
                                        coil_msg_log_body = f'{nww_id} set wrong bid_status to {my_full_name} in final acceptance: status: {nww_status}'
                                        coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                        await self.send(coil_msg_log)
                                        print(coil_msg_log_body)
                                else:
                                    """inform log of issue"""
                                    coil_msg_log_body = f'incorrect sender'
                                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                    await self.send(coil_msg_log)
                                    print(coil_msg_log_body)
                            else:
                                """inform log"""
                                coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                                await self.send(coil_msg_log)
                                print(coil_msg_log_body)
                        else:
                            """inform log of status"""
                            to_do = "search-auction"
                            nww_id = nww_coil_msg_df.loc[0, 'id']
                            not_entered_auctions += int(1)
                            entered_auction_str = f'{my_full_name} did not enter {nww_id} auction because sequencing rules do not match. Not_entered auction number: {not_entered_auctions}'
                            coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var, to_do, entered_auction_str).to_json()
                            coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                            await self.send(coil_msg_log)
                            print(entered_auction_str)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif coil_status_var == "sleep":
                """wait for message from in case fabrication was interrupted"""
                interrupted_fab_msg = await self.receive(timeout=wait_msg_time)
                if interrupted_fab_msg:
                    interrupted_fab_msg_sender = interrupted_fab_msg.sender
                    if interrupted_fab_msg_sender[:-33] == "bro":
                        interrupted_fab_msg_df = pd.read_json(interrupted_fab_msg)
                        if interrupted_fab_msg_df.loc[0, 'int_fab'] == 1:
                            coil_data_df.loc[0, 'int_fab'] = 1
                            coil_status_var = "stand-by"
                            """inform log of issue"""
                            this_time = datetime.datetime.now()
                            coil_msg_log_body = f'{my_full_name} interrupted fab. Received that msg at {this_time}'
                            coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                            await self.send(coil_msg_log)
                            print(coil_msg_log_body)
                    else:
                        """inform log"""
                        time.sleep(5)
                        coil_msg_log_body = f'{my_full_name} receive msg at {coil_status_var}, but not from browser'
                        coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    time.sleep(5)
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif coil_status_var == "stand-by":  # stand-by status for BR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won´t change to stand-by.
                """inform log of status"""
                coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                # We could introduce here a condition to be met to change to "on"
                # now it just changes directly to auction
                """inform log of status"""
                coil_status_var = "auction"
                coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
            else:
                """inform log of status"""
                coil_inform_json = opf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                coil_msg_log = opf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                coil_status_var = "stand-by"

        async def on_end(self):
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1

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
    parser.add_argument('-w', '--wait_auction_time', type=int, metavar='', required=False, default=500,
                        help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, off')
    parser.add_argument('-b', '--budget', type=int, metavar='', required=False, default=200, help='budget: in case of needed, budget can be increased')
    parser.add_argument('-l', '--location', type=str, metavar='', required=False, default='K',
                        help='location: K')
    parser.add_argument('-c', '--code', type=str, metavar='', required=False, default='cO202106101',
                        help='code: cO202106101')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    coil_started_at = datetime.datetime.now().time()
    coil_status_var = args.status
    refresh_time = datetime.datetime.now() + datetime.timedelta(seconds=1)
    """Save to csv who I am"""
    coil_data_df = opf.set_agent_parameters(my_dir, my_name, my_full_name)
    coil_data_df.at[0, 'budget'] = args.budget
    budget = coil_data_df.loc[0, 'budget']
    print(f'budget:{budget}')
    bid_register_df = opf.bid_register(my_name, my_full_name)
    not_entered_auctions = int(0)
    """XMPP info"""
    coil_jid = opf.agent_jid(my_dir, my_full_name)
    coil_passwd = opf.agent_passwd(my_dir, my_full_name)
    coil_agent = CoilAgent(coil_jid, coil_passwd)
    future = coil_agent.start(auto_register=True)
    future.result()
    """Counter"""
    #python coil.py -w 5 -st 5
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        coil_status_var = "off"
        coil_agent.stop()
        quit_spade()

# agent will live as set as stop_time and inform to log when it stops at status_var == "stand-by"
