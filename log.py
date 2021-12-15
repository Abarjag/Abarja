import time, datetime, sys, logging, argparse
import os, re , json, socket, aioxmpp, random
import getpass, globals, signal, pdb
#
import pandas as pd
import numpy as np
import operative_functions as opf
import logging.handlers as handlers
#
from spade import quit_spade
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
from os import remove
from aioxmpp import PresenceState, PresenceShow
#

global wait_msg_time, logger, log_status_var, active_agents, ip_machine, active_coil_agents

def handler(signum, frame):
    print('Signal handler called with signal', signum)
    log_agent.stop()

signal.signal(signal.SIGALRM,handler)

class LogAgent(Agent):
    class LogBehav(CyclicBehaviour):
        async def run(self):
            global wait_msg_time, logger, log_status_var, active_agents, ip_machine, active_coil_agents
            self.presence.on_subscribe = self.on_subscribe
            self.presence.on_unsubscribe = self.on_unsubscribe
            if log_status_var == "on":
                msg = await self.receive(timeout=wait_msg_time)  # wait for a message for 20 seconds
                if msg:
                    msg_sender_jid0 = str(msg.sender)
                    msg_sender_jid2 = msg_sender_jid0.split('/')[0]
                    agent_type = opf.aa_type(msg_sender_jid2)

                    '''Suscribe agents to contact list'''
                    self.presence.subscribe(msg_sender_jid0)

                    """Log file"""
                    fileh = logging.FileHandler(f'{my_dir}/{my_name}.log')
                    formatter = logging.Formatter(f'%(asctime)s;%(levelname)s;{agent_type};%(pathname)s;%(message)s')
                    fileh.setFormatter(formatter)
                    log = logging.getLogger()  # root logger
                    for hdlr in log.handlers[:]:  # remove all old handlers
                        log.removeHandler(hdlr)
                    log.addHandler(fileh)
                    if args.verbose == "DEBUG":
                        logger.setLevel(logging.DEBUG)
                    elif args.verbose == "INFO":
                        logger.setLevel(logging.INFO)
                    elif args.verbose == "WARNING":
                        logger.setLevel(logging.WARNING)
                    elif args.verbose == "ERROR":
                        logger.setLevel(logging.ERROR)
                    elif args.verbose == "CRITICAL":
                        logger.setLevel(logging.CRITICAL)
                    else:
                        print('not valid verbosity')

                    """Read msg purpose"""
                    msg_2 = pd.read_json(msg.body)
                    if not hasattr(msg_2,'purpose'):
                        logger.info(msg.body)
                    elif msg_2.loc[0, 'purpose'] == 'inform error':
                        logger.error(msg.body)
                    elif 'IP' in msg_2:
                        logger.debug(msg.body)
                    elif msg_2.loc[0, 'purpose'] == 'new_coil':
                        self.counter += 1
                        counter = int(self.counter)
                        """Active coil agents register"""
                        coil_id = msg_2.loc[0, 'coil_code']
                        coil_agent_name = msg_2.loc[0, 'agent_name']
                        coil_location = msg_2.loc[0, 'coil_location']
                        coil_register_df = [
                            {'coil_id': coil_id, 'coil_agent_name': coil_agent_name, \
                             'coil_jid': msg_sender_jid2, 'coil_location': coil_location}]
                        if (counter == 2):
                            active_coil_agents = pd.DataFrame([], columns=['coil_id', \
                                         'coil_agent_name', 'coil_jid', 'coil_location'])
                            active_coil_agents = active_coil_agents.append(coil_register_df,\
                                         ignore_index=True)
                        else:
                            active_coil_agents = active_coil_agents.append(coil_register_df,\
                                         ignore_index=True)
                            active_coil_agents = active_coil_agents.drop_duplicates([\
                                         'coil_id', 'coil_agent_name'], keep='first')
                            globals.act_coils = active_coil_agents

                    elif msg_2.loc[0, 'purpose'] == 'contact_list':
                        contacts = self.agent.presence.get_contacts()
                        cl_msg = [ str(j) for j in contacts ]
                        counter= int(msg_2.loc[0,'seq'])
                        rq_clist = opf.rq_list(my_full_name, cl_msg,\
                                    globals.gbrw_jid,'answer_clist',counter)
                        rq_clist_json = opf.contact_list_json(rq_clist,msg_sender_jid2)
                        await self.send(rq_clist_json)

                    elif  msg_2.loc[0, 'purpose'] == 'inform status':                          # TODO
                        if msg_2.loc[0, 'status'] == 'ended':
                            self.presence.unsubscribe(msg_sender_jid0)
                    elif 'active_coils' in msg_2:
                        logger.critical(msg.body)
                    else:
                        logger.info(msg.body)
                    """Update coil status """
                    x = re.search("won auction to process", msg.body)
                    if x:  # update  coil status
                        auction = msg.body.split(" ")
                        coil_id = auction[0]
                        status = auction[6]
                        if hasattr(globals,'agnts_full'):
                            opf.update_coil_status(coil_id, status)
                            updated_coil = f'Coil: {coil_id} updated status'
                            updated_coil = opf.update_coil_status(updated_coil)
                            logger.info(updated_coil)
                else:
                    msg = f"Log_agent didn't receive any msg in the last {wait_msg_time}s"
                    msg = json.dumps(msg)
                    logger.debug(msg)
            elif log_status_var == "stand-by":
                status_log = opf.log_status(my_full_name, log_status_var, ip_machine)
                logger.debug(status_log)

                log_status_var = "on"
                status_log = opf.log_status(my_full_name, log_status_var, ip_machine)
                logger.debug(status_log)
            else:
                status_log = opf.log_status(my_full_name, log_status_var, ip_machine)
                logger.debug(status_log)
                log_status_var = "stand-by"
                status_log = opf.log_status(my_full_name, log_status_var, ip_machine)
                logger.debug(status_log)

        async def on_end(self):
            # active_coil_agents.to_csv('coil_situation.csv', header=True, index=False)
            end_msg = opf.send_activation_finish(my_full_name,ip_machine,'end')
            logger.debug(end_msg)
            self.presence.unsubscribe(log_jid)
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1
            self._contacts = {}

        async def on_subscribe(self, jid):
            print("[{}] Agent {} asked for subscription. Let's aprove it.".format(self.agent.name, jid.split("@")[0]))
            self.presence.approve(jid)
            self.presence.subscribe(jid)

        async def on_unsubscribe(self, jid):
            print("[{}] Agent {} asked for UNsubscription. Let's aprove it.".format(self.agent.name, jid.split("@")[0]))
            self.presence.unsubscribe(jid)

    async def setup(self):
        b = self.LogBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)
        fileh = logging.FileHandler(f'{my_dir}/{my_name}.log')
        formatter = logging.Formatter(f'%(asctime)s;%(levelname)s;{my_full_name};%(pathname)s;%(message)s')
        fileh.setFormatter(formatter)
        log = logging.getLogger()  # root logger
        for hdlr in log.handlers[:]:  # remove all old handlers
            log.removeHandler(hdlr)
        log.addHandler(fileh)
        if args.verbose == "DEBUG":
            logger.setLevel(logging.DEBUG)
        elif args.verbose == "INFO":
            logger.setLevel(logging.INFO)
        elif args.verbose == "WARNING":
            logger.setLevel(logging.WARNING)
        elif args.verbose == "ERROR":
            logger.setLevel(logging.ERROR)
        elif args.verbose == "CRITICAL":
            logger.setLevel(logging.CRITICAL)
        else:
            print('not valid verbosity')

        "IP"
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip_machine = s.getsockname()[0]
        start_msg = opf.send_activation_finish(my_full_name, ip_machine, 'start')
        logger.debug(start_msg)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='Log parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1,
                        help='agent_number: 1,2,3,4..')
    parser.add_argument('-v', '--verbose', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        metavar='', required=False, default='DEBUG', help='verbose: amount of information saved')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=800,
                        help='wait_msg_time: time in seconds to wait for a msg. Purpose of system monitoring')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600,
                        help='stop_time: time in seconds where agent isnt asleep')
    parser.add_argument('-do', '--delete_order', type=str, metavar='', required=False, default='No',
                        help='Order to delete')  # 29/04
    parser.add_argument('-u', '--user_name', type=str, metavar='', required=False, default='No',
                        help='User to the XMPP platform')  # JOM 10/10
    parser.add_argument('-p', '--user_passwd', type=str, metavar='', required=False, default='No',
                        help='Passwd for the XMPP platform')  # JOM 10/10
    parser.add_argument('-bag', '--brw_agnt_id', type=str, metavar='', required=False, help='User ID for the browser agent')
    #
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    delete_order = args.delete_order
    my_full_name = args.user_name
    wait_msg_time = args.wait_msg_time
    log_status_var = "stand-by"

    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]

    """Logger info"""
    logger = logging.getLogger(__name__)

    """XMPP info"""
    if hasattr(args,'brw_agnt_id') :
        gbrw_jid = args.brw_agnt_id
        globals.gbrw_jid = gbrw_jid
    if len(args.user_name) > 0:
        log_jid = args.user_name
        globals.glog_jid = log_jid
    else:
        log_jid = opf.agent_jid(my_dir, my_full_name)
        globals.glog_jid = log_jid
    if len(args.user_passwd) > 0:
        log_passwd = args.user_passwd
    else:
        log_passwd = opf.agent_passwd(my_dir, my_full_name)
    log_agent = LogAgent(log_jid, log_passwd)
    future = log_agent.start(auto_register=True)
    future.result()

    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        log_agent.stop()
        log_status_var = "off"
        stop_msg_log = f"{my_full_name}_agent stopped, coil_status_var: {log_status_var}"
        stop_msg_log = json.dumps(stop_msg_log)
        logger.critical(stop_msg_log)
        quit_spade()
        # while 1:
