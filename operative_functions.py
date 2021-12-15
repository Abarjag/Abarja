import os, datetime, subprocess, pdb
import math, json, time, statistics, globals
import numpy as np
import pandas as pd
from datetime import timedelta,date
from random import random
from random import randrange
from random import randint
from spade.message import Message

"""General Functions"""

def order_file(agent_full_name, order_code, steel_grade, thickness, \
               width_coils, num_coils, list_coils, list_lengths, param_f, \
               each_coil_price, list_ware, string_operations, prev_station, wait_time, shdate):
    order_msg_log = pd.DataFrame([], columns=['id','From', 'order_code', \
                       'steel_grade', 'thickness_coils', 'width_coils', \
                       'num_coils', 'list_coils', 'list_lengths', \
                       'each_coil_price', 'string_operations', 'date'])
    #
    order_msg_log.at[0, 'id'] = agent_full_name
    order_msg_log.at[0, 'purpose'] = 'setup'
    order_msg_log.at[0, 'msg'] = 'new order'
    order_msg_log.at[0, 'order_code'] = order_code
    order_msg_log.at[0, 'steel_grade'] = steel_grade
    order_msg_log.at[0, 'thickness_coils'] = thickness
    order_msg_log.at[0, 'width_coils'] = width_coils
    order_msg_log.at[0, 'num_coils'] = num_coils
    order_msg_log.at[0, 'list_coils'] = list_coils
    order_msg_log.at[0, 'list_lengths'] = list_lengths
    order_msg_log.at[0, 'each_coil_price'] = each_coil_price
    order_msg_log.at[0, 'list_ware'] = list_ware
    order_msg_log.at[0, 'prev_st'] = prev_station
    order_msg_log.at[0, 'string_operations'] = string_operations
    order_msg_log.at[0, 'date'] = date.today().strftime('%Y-%m-%d')
    order_msg_log.at[0, 'to'] = 'log'
    order_msg_log.at[0, 'wait_time'] = wait_time
    order_msg_log.at[0, 'ship_date'] = shdate
    order_msg_log.at[0, 'param_f'] = param_f
    return order_msg_log

def order_to_log(order_body,agent_directory):
    glog_jid = globals.glog_jid
    if len(glog_jid) > 0:
        log_jid = glog_jid
    else:
        agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == "log"]
        log_jid = agents_df['User name'].iloc[-1]
    order_msg = Message(to=log_jid)
    order_msg.body = order_body
    order_msg.set_metadata("performative", "inform")
    return order_msg

def save_order(msg):
    mj = json.loads(msg)[0]
    code  = mj['order_code']
    steel = mj['steel_grade']
    thick = mj['thickness_coils']
    width = mj['width_coils']
    num   = mj['num_coils']
    lst   = [jj.strip() for jj in mj['list_coils'].split(',')]
    lplc  = [jj.strip() for jj in mj['list_ware'].split(',')]
    price = mj['each_coil_price']
    msg   = mj['msg']
    dat   = mj['date']
    strgop= mj['string_operations']
    status= [jj.strip() for jj in mj['string_operations'].split('|')]
    lista_total =[]
    columns = ['Date','Order_code', 'Steel_grade', 'Thickness', 'Width_coils', \
               'Number_coils','ID_coil', 'L_coil', 'Price_coils', 'Operations',\
               'coil_status']
    for i in range(num):
        lista_total.append({'Date': dat, 'Order_code': code,\
            'Steel_grade': steel, 'Thickness': thick, 'Width_coils': width,\
            'Number_coils': num, 'ID_coil': lst[i], 'Price_coils': price,\
            'Operations': strgop,'L_coil': lplc[i], 'coil_status': status[0]})
    df = pd.DataFrame(lista_total, columns=columns)
    if hasattr(globals,'orders') :
        gdf = pd.concat([globals.orders,df],ignore_index=True)
        globals.orders = gdf
    else:
        globals.orders = df
    return(df)

def order_to_search(search_body,agent_full_name , agent_directory):
    gbrw_jid = globals.gbrw_jid
    if len(gbrw_jid) > 0:
        browser_jid = gbrw_jid
    else:
        agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == "browser"]
        browser_jid = agents_df['User name'].iloc[-1]
    search_msg = Message(to=browser_jid)
    search_msg.body = search_body
    search_msg.set_metadata("performative", "inform")
    return search_msg

def order_searched(filter,agent_request,agent_directory):
    glhr_jid = globals.glhr_jid
    if len(glhr_jid) > 0:
        launcher_jid = glhr_jid
    else:
        agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == agent_request]
        launcher_jid = agents_df['User name'].iloc[-1]
    order_searched_msg = Message(to=launcher_jid)
    order_searched_msg.body = 'Order searched:'+ filter
    order_searched_msg.set_metadata("performative","inform")
    return order_searched_msg

def order_coil(la_json, code, dact):
    name = dact.loc[dact['code'] == code, 'id'].values
    name = name[0]
    msg_budget = Message()
    msg_budget.to = str(name)
    msg_budget.body = la_json
    return msg_budget

def order_budget(budget, code, dact):
    df = pd.DataFrame()
    coil = dact[dact['code'] == code]
    name = dact.loc[dact['code'] == code, 'id'].values
    name = name[0]

    df.loc[0, 'id'] = 'launcher'
    df.loc[0, 'purpose'] = 'update'
    df.loc[0, 'msg'] = coil.to_json(orient='records')
    df.loc[0, 'new_budget'] = budget
    df.loc[0, 'to'] = str(name)
    return df

def update_coil_status(coil_id, status):
    # df = pd.read_csv('RegisterOrders.csv',header=0,delimiter=",",engine='python')
    df   = globals.orders
    df.loc[(df.ID_coil.isin([coil_id])), 'coil_status'] = status
    # df.to_csv('RegisterOrders.csv', index = False)
    globals.orders = df

#
def agents_data():
    """This is a file from which all functions read information. It contains crucial information of the system:
    -jids, passwords, agent_full_names, pre-stablished location and WH capacity"""
    # d = {'User name': ['log@apiict03.etsii.upm.es', 'browser@apiict03.etsii.upm.es',
    #                    'ca01@apiict03.etsii.upm.es', 'ca02@apiict03.etsii.upm.es', 'ca03@apiict03.etsii.upm.es', 'ca04@apiict03.etsii.upm.es',
    #                    'tc01@apiict03.etsii.upm.es', 'tc02@apiict03.etsii.upm.es', 'tc03@apiict03.etsii.upm.es',
    #                    'st01@apiict03.etsii.upm.es', 'st02@apiict03.etsii.upm.es', 'st03@apiict03.etsii.upm.es', 'st04@apiict03.etsii.upm.es', 'st05@apiict03.etsii.upm.es',
    #                    'c001@apiict03.etsii.upm.es', 'c002@apiict03.etsii.upm.es', 'c003@apiict03.etsii.upm.es', 'c004@apiict03.etsii.upm.es', 'c005@apiict03.etsii.upm.es',
    #                    'c006@apiict03.etsii.upm.es', 'c007@apiict03.etsii.upm.es', 'c008@apiict03.etsii.upm.es', 'c009@apiict03.etsii.upm.es', 'c010@apiict03.etsii.upm.es',
    #                    'c011@apiict03.etsii.upm.es', 'c012@apiict03.etsii.upm.es', 'c013@apiict03.etsii.upm.es', 'c014@apiict03.etsii.upm.es', 'c015@apiict03.etsii.upm.es',
    #                    'c016@apiict03.etsii.upm.es', 'c017@apiict03.etsii.upm.es', 'c018@apiict03.etsii.upm.es', 'c019@apiict03.etsii.upm.es', 'c020@apiict03.etsii.upm.es',
    #                    'c021@apiict03.etsii.upm.es', 'c022@apiict03.etsii.upm.es', 'c023@apiict03.etsii.upm.es', 'c024@apiict03.etsii.upm.es', 'c025@apiict03.etsii.upm.es'],
    #      'Password': ['DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynReact', 'DynRe>    #      'Name': ['log', 'browser',
    #               'ca_01', 'ca_02', 'ca_03', 'ca_04',
    #               'tc_01', 'tc_02', 'tc_03',
    #               'wh_01', 'wh_02', 'wh_03', 'wh_04', 'wh_05',
    #               'coil_001', 'coil_002', 'coil_003', 'coil_004', 'coil_005', 'coil_006', 'coil_007', 'coil_008',
    #               'coil_009', 'coil_010', 'coil_011', 'coil_012', 'coil_013', 'coil_014', 'coil_015', 'coil_016',
    #               'coil_017', 'coil_018', 'coil_019', 'coil_020', 'coil_021', 'coil_022', 'coil_023', 'coil_024', 'coil_025'],
    #      'Location1': ['', '',
    #                    'A', 'C', 'E', 'G',
    #                    '', '', '',
    #                    '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '', ''],
    #      'Location2': ['', '',
    #                    'B', 'D', 'F', 'H',
    #                    '', '', '',
   #                    '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '',
    #                    '', '', '', '', '', '', '', '', ''],
    #      'Location': ['', 'X',
    #                   'A-B', 'C-D', 'E-F', 'G-H',
    #                   'I', 'K', 'M',
    #                   'I', 'J', 'K', 'L', 'M',
    #                   'I', 'J', 'K', 'I', 'J', 'K', 'J', 'I',
    #                   'K', 'L', 'I', 'M', 'I', 'M', 'J', 'I',
    #                   'M', 'I', 'K', 'L', 'J', 'J', 'L', 'K', 'I'],
    #      'Capacity': ['', '',
    #                   '', '', '', '',
    #                   '', '', '',
    #                   8, 9, 5, 5, 3,
    #                   '', '', '', '', '', '', '', '',
    #                   '', '', '', '', '', '', '', '',
    #                   '', '', '', '', '', '', '', '', '']}
    #
    agents_data_df = pd.DataFrame()
    globals.agnts_full = agents_data_df
    return agents_data_df


def agent_jid(agent_directory, agent_full_name):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    jid_direction = agents_df['User name'].iloc[-1]
    return jid_direction


def agent_passwd(agent_directory, agent_full_name):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    password = agents_df['Password'].iloc[-1]
    return password

def my_full_name(agent_name, agent_number):
    decimal = ""
    if agent_name == "coil":
        if len(str(agent_number)) == 1:
            decimal = str("00")
        elif len(str(agent_number)) == 2:
            decimal = str(0)
        full_name = str(agent_name) + str("_") + decimal + str(agent_number)
    elif agent_name == "log":
        full_name = agent_name
    elif agent_name == "browser":
        full_name = agent_name
    elif agent_name == "launcher": #todo
        full_name = agent_name
    else:
        if len(str(agent_number)) == 1:
            decimal = str(0)
        elif len(str(agent_number)) == 2:
            decimal = ""
        full_name = str(agent_name) + str("_") + decimal + str(agent_number)
    return full_name


def activation_df(agent_full_name, status_started_at, agentid, *args):
    agent_data_df = pd.DataFrame([],columns=['id','agent_type','location_1',\
                    'location_2','location','purpose','request_type', \
                    'time','activation_time'])
    act_df = agent_data_df.loc[:, 'id':'activation_time']
    act_df = act_df.astype(str)
    act_df.at[0, 'purpose'] = "inform"
    act_df.at[0, 'request_type'] = ""
    act_df.at[0, 'time'] = datetime.datetime.now()
    act_df.at[0, 'status'] = "on"
    act_df.at[0, 'activation time'] = status_started_at
    act_df.at[0, 'agent_type'] = agent_full_name
    act_df.at[0, 'id'] = agentid
    if args:
        df = args[0]
        act_df = act_df.join(df)
    act_json = act_df.to_json(orient="records")
    return act_json


def inform_log_df(agent_full_name,typea, status_started_at, status, *args, **kwargs):
    """Inform of agent status"""
    inf_df = pd.DataFrame([],columns=['id','agent_type',\
                      'location_1','location_2','location','purpose', \
                      'request_type','time','activation_time','IP'])
    inf_df = inf_df.astype(str)
    inf_df.at[0, 'id'] = agent_full_name
    inf_df.at[0, 'agent_type'] = typea
    inf_df.at[0, 'purpose'] = "inform"
    inf_df.at[0, 'request_type'] = ""
    inf_df.at[0, 'time'] = datetime.datetime.now()
    inf_df.at[0, 'status'] = status
    inf_df.at[0, 'activation time'] = status_started_at
    inf_df.at[0, 'IP'] = globals.IP
    #
    # In the case of stand-by coil, it passes to_do = "searching_auction" so that browser
    # can search this coil when a resource looks for processing.
    if args:
        inf_df.at[0, 'to_do'] = args[0].to_json(orient="records")
    if kwargs:  # in case did not enter auction
        inf_df.at[0, 'entered_auction'] = kwargs[0]
    return inf_df


def msg_to_log(msg_body, agent_directory=''):
    glog_jid = globals.glog_jid
    if len(glog_jid) > 0:
        log_jid = glog_jid
    else:
        if hasattr(globals,'agnts_full') :
            agents_df = globals.agnts_full
        else:
            agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == "log"]
        log_jid = agents_df['User name'].iloc[-1]
    msg_log = Message(to=log_jid)
    msg_log.body = msg_body
    msg_log.set_metadata("performative", "inform")
    return msg_log

def msg_to_agnt(msg_body,agent):
    msg_log = Message(to=agent)
    msg_log.body = msg_body
    msg_log.set_metadata("performative", "inform")
    return msg_log

def msg_to_sender(received_msg):
    """Returns msg to send without msg.body"""
    msg_reply = Message()
    msg_reply.to = str(received_msg.sender)
    msg_reply.set_metadata("performative", "inform")
    return msg_reply


def auction_blank_df():
    """Returns df column structure with all necessary information to evaluate auction performance"""
    df = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2',\
                    'location','coil_auction_winner', 'largo', 'ancho', \
                    'espesor', 'peso','int_fab', 'bid', 'budget', \
                    'ship_date', 'ship_date_rating','setup_speed', 'T1', 'T2', 'T3', \
                    'T4', 'T5', 'q', 'T1dif', 'T2dif', 'T3dif', 'T4dif', 'T5dif', \
                    'total_temp_dif', 'temp_rating','bid_rating', 'int_fab_priority', \
                    'int_fab_rating', 'rating', 'rating_dif', 'negotiation', \
                    'pre_auction_start', 'auction_start', 'auction_finish', \
                    'active_tr_slot_1', 'active_tr_slot_2', 'tr_booking_confirmation_at', \
                    'active_wh', 'wh_booking_confirmation_at', 'wh_location', \
                    'active_coils', 'auction_coils','brAVG(tr_op_time)', \
                    'brAVG(ca_op_time)', 'AVG(tr_op_time)', 'AVG(ca_op_time)', \
                    'fab_start','slot_1_start', 'slot_1_end', 'slot_2_start', \
                    'slot_2_end', 'name_tr_slot_1', 'name_tr_slot_2', 'delivered_to_wh', \
                    'handling_cost_slot_1', 'handling_cost_slot_2','coil_ratings_1', \
                    'coil_ratings_2','pre_auction_duration', 'auction_duration',\
                    'gantt', 'location_diagram'])
    return df


def set_agent_parameters(agent_name, agent_full_name,ancho,espesor,\
                         largo,param_f,sgrade,location,ordr):
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                 'location_2', 'location', 'purpose', 'request_type', \
                 'time', 'activation_time', 'int_fab'])
    agent_data.at[0, 'id'] = agent_full_name
    agent_data.at[0, 'agent_type'] = agent_name
    if hasattr(globals,'agnts_full'):
        agents_df = globals.agnts_full
        if agents_df.shape[0] > 0:
            agents_df = agents_df.loc[agents_df['User name'] == agent_full_name]
            agents_df = agents_df.reset_index(drop=True)
    else:
        agents_df = pd.DataFrame([], columns=['User name','Password','Name',\
                                 'Location1','Location2','Location','Capacity',\
                                 'From','Code'])
        globals.agnts_full = agents_df
    if agent_name == 'ca':
        agent_data = agent_data.reindex(columns=['id', 'agent_type', \
                     'location_1', 'location_2', 'location', 'purpose', \
                     'request_type', 'time', 'activation_time', \
                     'setup_speed', 'T1', 'T2', 'T3', 'T4', 'T5', 'q'])
        agent_data = ca_parameters(agent_data, agents_df, agent_name)
    elif agent_name == "wh":
        if agents_df.shape[0] > 0:
            agent_data.at[0, 'location'] = agents_df.loc[0, 'Location']
            agent_data.at[0, 'capacity'] = agents_df.loc[0, 'Capacity']
            agent_data.at[0, 'load'] = 0
        else:
            agent_data.at[0, 'location'] = ''
            agent_data.at[0, 'capacity'] = ''
            agent_data.at[0, 'load'] = 0
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location_1', 'location_2', \
                     'location', 'purpose', 'request_type', 'time', \
                     'activation_time', 'coil_in', 'coil_out', 'rack', \
                     'capacity', 'load'])
    elif agent_name == "coil":
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location_1', 'location_2', \
                     'location', 'purpose', 'request_type', 'time', \
                     'activation_time', 'to_do', 'entered_auction', \
                     'int_fab', 'bid', 'bid_status', 'largo', \
                     'ancho', 'espesor', 'peso', \
                     'number_auction', 'setup_speed', 'budget', \
                     'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'ship_date'])
        agent_data = coil_parameters(agent_data, agents_df, agent_name)
    elif agent_name == "nww":
        agent_data = agent_data.reindex(columns=['id', 'agent_type', \
                     'purpose', 'request_type', 'time', 'activation_time', \
                     'setup_speed', 'espesor', 'largo', \
                     'ancho','sgrade','From','order'])
        #
        agent_data.loc[0, 'ancho'] = ancho
        agent_data.loc[0, 'espesor'] = espesor
        agent_data.loc[0, 'largo'] = largo
        agent_data.loc[0, 'sgrade'] = sgrade
        agent_data.loc[0, 'From'] = location
        agent_data.loc[0, 'order'] = ordr
        agent_data.loc[0, 'param_f'] = param_f
        agent_data.loc[0, 'F_group'] = F_groups(param_f, agent_full_name)


    elif agent_name == "va":
        agent_data = agent_data.reindex(columns=['id', 'agent_type', \
                     'purpose', 'request_type', 'time', 'activation_time', \
                     'setup_speed', 'coil_thickness', 'coil_length', \
                     'coil_width','sgrade','location','order'])
        #
        agent_data.loc[0, 'coil_width'] = ancho
        agent_data.loc[0, 'coil_thickness'] = espesor
        agent_data.loc[0, 'coil_length'] = largo
        agent_data.loc[0, 'sgrade'] = sgrade
        agent_data.loc[0, 'localtion'] = location
        agent_data.loc[0, 'order'] = ordr

    elif agent_name == "tc":
        agent_data.loc[0, 'location'] = ''
    elif agent_name == "launcher":
        agent_data.loc[0, 'location'] = ''
    elif agent_name == "browser":
        agent_data.loc[0, 'location'] = ''
    else:
        agents_df = globals.agnts_full
        df = agents_df.loc[agents_df['Name'] == agent_name]
        df = df.reset_index(drop=True)
        if df.shape[0] > 0:
            agent_data.loc[0, 'location'] = df.at[0, 'Location']
        else:
            agent_data.loc[0, 'location'] = ''
    globals.agents = agent_data
    return agent_data

"""Agent-specific Functions"""

def conf_medidas(agent_df, configuracion_med):
    agent_df.at[0, 'coil_width'] = configuracion_med.loc[0, 'coil_width']
    agent_df.at[0, 'coil_length'] = configuracion_med.loc[0, 'coil_length']
    agent_df.at[0,'coil_thickness']= configuracion_med.loc[0,'coil_thickness']
    agent_df.at[0,'purpose']= 'bid'
    agent_df.at[0,'request_type']= 'pre-auction'
    return agent_df

def va_parameters(agent_data, agents_df, agent_name):
    #  Sets pseudo random parameters
    rn = random()
    agent_data.at[0, 'coil_width'] = 5 + (rn * 10)  # between 5-15
    agent_data.at[0, 'coil_length'] = 6 + (rn * 10)  # between 6-16
    agent_data.at[0, 'coil_thickness'] = 7 + (rn * 10)  # between 7-17
    return agent_data

# Not used anymore. Register is in log_agent
# def wh_create_register(agent_directory, agent_full_name):
#     # wh registers entrance and exit of coils as well as reservations.
#     agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
#     wh_register_df = agent_data_df.drop(agent_data_df.columns.difference(['select columns to drop']), 1, inplace=True)
#     wh_register_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}_register.csv', index=False, header=True)


def ca_parameters(agent_data, agents_df, agent_name):
    """Sets pseudo random parameters"""
    rn = random()
    agent_data.at[0, 'location_1'] = agents_df.loc[0, 'Location1']
    agent_data.at[0, 'location_2'] = agents_df.loc[0, 'Location2']
    agent_data.at[0, 'location'] = agents_df.loc[0, 'Location']
    agent_data.at[0, 'T1'] = 250 + (rn * 100)  # between 250-350
    agent_data.at[0, 'T2'] = 550 + (rn * 100)  # between 550-650
    agent_data.at[0, 'T3'] = 800 + (rn * 100)  # between 800-900
    agent_data.at[0, 'T4'] = 600 + (rn * 100)  # between 600-700
    agent_data.at[0, 'T5'] = 300 + (rn * 100)  # between 300-400
    agent_data.at[0, 'q'] = 0.5 + (rn / 10)  # between 05-0.6
    return agent_data



def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def wh_capacity_check(agent_full_name, agent_directory):
    """Checks load on WH agent. Returns a str that can be used as body of msg"""
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                    'location_2', 'location', 'purpose', 'request_type', 'time', \
                    'activation_time', 'int_fab'])
    if agent_data_df.loc[0, 'capacity'] == agent_data_df.loc[0, 'load']:
        msg_body = "negative"
    elif agent_data_df.loc[0, 'capacity'] > agent_data_df.loc[0, 'load']:
        msg_body = "positive"
    else:
        msg_body = "negative"
    return msg_body


def wh_append_booking(agent_full_name, agent_directory, ca_df):
    """Adds +1 to load of WH and registers reservation"""
    #
    agent_data_df = globals.agents
    agent_data_df.loc[0, 'load'] = int(agent_data_df.loc[0, 'load']) + 1
    globals.agents = agent_data_df
    #
    # create msg body to send to log with booking info
    data_to_save = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                    'location_2', 'purpose', 'request_type', 'time', 'rack', \
                    'coil_in', 'coil_out', 'capacity', 'load'])
    data_to_save.at[0, 'id'] = ca_df.loc[0, 'id']
    data_to_save.at[0, 'agent_type'] = ca_df.loc[0, 'agent_type']
    data_to_save.at[0, 'location_1'] = ca_df.loc[0, 'location_1']
    data_to_save.at[0, 'location_2'] = ca_df.loc[0, 'location_2']
    data_to_save.at[0, 'location'] = ca_df.loc[0, 'location']
    data_to_save.at[0, 'purpose'] = ca_df.loc[0, 'purpose']
    data_to_save.at[0, 'request_type'] = ca_df.loc[0, 'action']  # action=book
    data_to_save.at[0, 'time'] = datetime.datetime.now()
    data_to_save.at[0, 'rack'] = 1
    return data_to_save.to_json()


def wh_register(agent_full_name, agent_df):
    data_to_save = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                    'location_2', 'purpose', 'request_type', 'time', 'rack', \
                    'coil_in', 'coil_out', 'capacity', 'load'])
    data_to_save.at[0, 'id'] = agent_df.loc[0, 'id']
    data_to_save.at[0, 'time'] = datetime.datetime.now()
    data_to_save.at[0, 'rack'] = 1
    data_to_save.at[0, 'request_type'] = agent_df.loc[0, 'action']
    if agent_df.loc[0, 'action'] == "out":
        # append coil entrance to wh_register
        data_to_save.at[0, 'coil_out'] = agent_df[0, 'coil_out']  # from received ca_df
        #
        # discount load on agent_data_df
        agent_data_df = globals.agents
        agent_data_df.loc[0, 'load'] = int(agent_data_df.loc[0, 'Load']) - 1
        data_to_save.at[0, 'load'] = agent_data_df.loc[0, 'load']
    elif agent_df.loc[0, 'action'] == "in":
        # append coil entrance to wh_register
        data_to_save.at[0, 'coil_in'] = agent_df[0, 'coil_in']  # from received ca_df
        #
        # We don      t count +1 when coil enters on wh,
        # we already counted +1 at the booking.
    return data_to_save.to_json()


def tr_create_booking_register(agent_directory, agent_full_name):
    tr_register_df = pd.DataFrame([], columns=['day_minute', 'booking_type', \
                        'assigned_to', 'assigned_at'])
    for i in range(1440):
        tr_register_df.at[i, 'day_minute'] = i + 1
    globals.tr_register_df = tr_register_df


def slot_to_minutes(agent_df):
    """It generates a list with the time passed to minute number of the day.
    Each day has 1440 minutes. min 1 = 00.00h, minute 1440 = 23.59h"""
    slot_range = []
    if agent_df.loc[0, 'slot'] == 1:
        # transform data to datetime type
        agent_df['slot_1_start'] = pd.to_datetime(agent_df['slot_1_start'], unit='ms')
        agent_df['slot_1_end'] = pd.to_datetime(agent_df['slot_1_end'], unit='ms')
        slot_1_start = agent_df['slot_1_start']
        slot_1_end = agent_df['slot_1_end']
        # transform datetime to minute of the day
        slot_1_start_min = math.floor((int(agent_df.loc[0, 'slot_1_start'].strftime("%H"\
                            )) * 60) + (int(agent_df.loc[0, 'slot_1_start'].strftime("%M"\
                            ))) + (int(agent_df.loc[0, 'slot_1_start'].strftime("%S")) / 60))
        slot_1_end_min = math.ceil((int(agent_df.loc[0, 'slot_1_end'].strftime("%H")) * \
                            60) + (int(agent_df.loc[0, 'slot_1_end'].strftime("%M"))) + \
                            (int(agent_df.loc[0, 'slot_1_end'].strftime("%S")) / 60))
        slot_range = list(range(slot_1_start_min, slot_1_end_min+1))
    elif agent_df.loc[0, 'slot'] == 2:
        # transform data to datetime type
        agent_df['slot_2_start'] = pd.to_datetime(agent_df['slot_2_start'], unit='ms')
        agent_df['slot_2_end'] = pd.to_datetime(agent_df['slot_2_end'], unit='ms')
        # transform datetime to minute of the day
        slot_2_start_min = math.floor((int(agent_df.loc[0, 'slot_2_start'].strftime("%H")) * \
                            60) + (int(agent_df.loc[0, 'slot_2_start'].strftime("%M"))) + \
                            (int(agent_df.loc[0, 'slot_2_start'].strftime("%S")) / 60))
        slot_2_end_min = math.ceil((int(agent_df.loc[0, 'slot_2_end'].strftime("%H")) * 60) + \
                            (int(agent_df.loc[0, 'slot_2_end'].strftime("%M"))) + \
                            (int(agent_df.loc[0, 'slot_2_end'].strftime("%S")) / 60))
        # create a list with the slot_ranges that need to be pre-booked
        slot_range = list(range(slot_2_start_min, slot_2_end_min))
    return slot_range

def tr_check_availability(agent_directory, agent_full_name, slot_range):
    """Checks availability of tr agent and returns a positive or negative msg"""
    tr_create_booking_register(agent_directory, agent_full_name)
    # CHANGE THIS WHEN POSSIBLE. IT IS ERRASING ALL BOOKINGS.
    # NOW THE SYSTEM IS NOT CONSTRAINT IN TR RESOURCES.
    tr_booking_df = globals.tr_booking_df
    tr_booking_df['booking_type'] = tr_booking_df['booking_type'].fillna("")
    # Creates 2 lists: booked_slots_list & free_slots_list and checks availability.
    free_slots_list = []
    booked_slots_list = []
    prebooked_slots_list = []
    for x in slot_range:
        if tr_booking_df.loc[x - 1, 'booking_type'] == "pre-book":
            prebooked_slots_list.append(x)
        elif tr_booking_df.loc[x - 1, 'booking_type'] == "booked":
            booked_slots_list.append(x)
        else:
            free_slots_list.append(x)
    # Checks availability
    if len(booked_slots_list) >= 1:
        tr_msg_ca_body = "negative"
    else:
        tr_msg_ca_body = "positive"
    globals.tr_booking_df = tr_booking_df
    return tr_msg_ca_body


def tr_append_booking(agent_directory, agent_full_name, agent_df, slot_range):
    """Appends pre-booking or booking to booking register and returns booking info as a json"""
    tr_booking_df = globals.tr_booking_df
    tr_booking_df['booking_type'] = tr_booking_df['booking_type'].fillna("")
    for y in slot_range:
        tr_booking_df.loc[y - 1, 'assigned_to'] = agent_df.loc[0, 'id']
        tr_booking_df.loc[y - 1, 'assigned_at'] = datetime.datetime.now()
        if agent_df.loc[0, 'action'] == "booked":
            tr_booking_df.loc[y - 1, 'booking_type'] = "booked"
        elif agent_df.loc[0, 'action'] == "pre-book":
            tr_booking_df.loc[y - 1, 'booking_type'] = "pre-book"
        tr_booking_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}_booking.csv', \
                             index=False, header=True)
    globals.tr_booking_df = tr_booking_df
    return tr_booking_df.to_json()


def req_active_users_loc_times(agent_df, *args):
    # Returns msg body to send to browser as a json
    ca_request_df = agent_df.loc[:, 'id':'time']
    ca_request_df = ca_request_df.astype(str)
    ca_request_df.at[0, 'purpose'] = "request"
    this_time = datetime.datetime.now()
    ca_request_df.at[0, 'time'] = this_time
    if args:
        ca_request_df.at[0, 'request_type'] = args[0]
    else:
        ca_request_df.at[0, 'request_type'] = "active users location & op_time"
    return ca_request_df


def msg_to_br(msg_body, agent_directory):
    """Returns msg object to send to browser agent"""
    gbrw_jid = globals.gbrw_jid
    if len(gbrw_jid) > 0:
        jid = gbrw_jid
    else:
        agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == "browser"]
        jid = agents_df['User name'].iloc[-1]
    msg_br = Message(to=jid)
    msg_br.body = msg_body
    msg_br.set_metadata("performative", "inform")
    return msg_br


def br_jid(agent_directory):
    """Returns str with browser jid"""
    gbrw_jid = globals.gbrw_jid
    if len(gbrw_jid) > 0:
        jid = gbrw_jid
    else:
        agents_df = agents_data()
        agents_df = agents_df.loc[agents_df['Name'] == "browser"]
        jid = agents_df['User name'].iloc[-1]
    return jid


def estimate_tr_slot(br_data_df, fab_started_at, leeway, agent_df):
    """Returns a df with the the calculated time slots for which tr is requested"""
    a = br_data_df.loc[0, 'AVG(ca_op_time)']
    b = br_data_df.loc[0, 'AVG(tr_op_time)']
    ca_estimated_end = fab_started_at + datetime.timedelta(minutes=int(br_data_df.loc[0, \
                'AVG(ca_op_time)']))  # time when on going fab started + mean ca processing time.
    if br_data_df.loc[0, 'AVG(ca_op_time)'] == 9:
        if br_data_df.loc[0, 'AVG(tr_op_time)'] == 3.5:
            slot_1_start = ca_estimated_end - datetime.timedelta(minutes=int(\
                br_data_df.loc[0, 'AVG(tr_op_time)'])) - (leeway / 2)
            slot_1_end = ca_estimated_end + (leeway / 2)

    slot_1_start = ca_estimated_end - datetime.timedelta(minutes=int(br_data_df.loc[\
                0, 'AVG(tr_op_time)'])) - (leeway / 2)
    slot_1_end = ca_estimated_end + (leeway / 2)
    slot_1_start = ca_estimated_end - datetime.timedelta(minutes=int(br_data_df.loc[\
                0, 'AVG(tr_op_time)'])) - (leeway / 2)
    slot_1_end = ca_estimated_end + (leeway / 2)
    slot_2_start = ca_estimated_end + datetime.timedelta(minutes=int(br_data_df.loc[\
                0, 'AVG(ca_op_time)'])) - datetime.timedelta(minutes=int(br_data_df.loc[\
                0, 'AVG(tr_op_time)']/2)) - (leeway / 2)
    slot_2_end = ca_estimated_end + datetime.timedelta(minutes=int(br_data_df.loc[0, \
                'AVG(ca_op_time)'])) + datetime.timedelta(minutes=int(br_data_df.loc[\
                0, 'AVG(tr_op_time)']/2)) + (leeway / 2)
    # time when on going fab started + mean ca processing time + mean tr operation time
    ca_to_tr_df = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                'location_2', 'location', 'purpose', 'request_type', 'action', \
                'time', 'slot_1_start', 'slot_1_end', 'slot_2_start', 'slot_2_end', \
                'slot'])
    ca_to_tr_df.at[0, 'id'] = agent_df.loc[0, 'id']
    ca_to_tr_df.at[0, 'agent_type'] = agent_df.loc[0, 'agent_type']
    ca_to_tr_df.at[0, 'location_1'] = agent_df.loc[0, 'location_1']
    ca_to_tr_df.at[0, 'location_2'] = agent_df.loc[0, 'location_2']
    ca_to_tr_df.at[0, 'location'] = agent_df.loc[0, 'location']
    ca_to_tr_df.at[0, 'purpose'] = "request"
    ca_to_tr_df.at[0, 'slot_1_start'] = slot_1_start
    ca_to_tr_df.at[0, 'slot_1_end'] = slot_1_end
    ca_to_tr_df.at[0, 'slot_2_start'] = slot_2_start
    ca_to_tr_df.at[0, 'slot_2_end'] = slot_2_end
    this_time = datetime.datetime.now()
    ca_to_tr_df.at[0, 'time'] = this_time
    ca_to_tr_df.at[0, 'request_type'] = "request"
    ca_to_tr_df.at[0, 'action'] = "pre-book"
    return ca_to_tr_df


def handling_cost(ca_to_tr_df, slot):
    slot_total_minutes = ""
    if slot == 1:
        slot_total_minutes = ca_to_tr_df.at[0, 'slot_1_end'] - \
                             ca_to_tr_df.at[0, 'slot_1_start']
    elif slot == 2:
        slot_total_minutes = ca_to_tr_df.at[0, 'slot_2_end'] - \
                             ca_to_tr_df.at[0, 'slot_2_start']

    handling_cost = slot_total_minutes.total_seconds() * (50 / 3600)  # 50   ^b   /h
    return handling_cost


def locations_min_distances():
    d = {'id_min': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
         'location_A': ['A-A', 'A-C', 'A-E', 'A-G', 'A-I', 'A-M', 'A-J', 'A-B', 'A-D', 'A-F', 'A-H', 'A-K', 'A-L'],
         'location_B': ['B-B', 'B-D', 'B-F', 'B-H', 'B-K', 'B-L', 'B-J', 'B-A', 'B-C', 'B-E', 'B-G', 'B-I', 'B-M'],
         'location_C': ['C-C', 'C-A', 'C-E', 'C-G', 'C-I', 'C-M', 'C-J', 'C-B', 'C-D', 'C-F', 'C-H', 'C-K', 'C-L'],
         'location_D': ['D-D', 'D-B', 'D-F', 'D-H', 'D-K', 'D-L', 'D-J', 'D-A', 'D-C', 'D-G', 'D-E', 'D-I', 'D-M'],
         'location_E': ['E-E', 'E-B', 'E-C', 'E-A', 'E-M', 'E-I', 'E-L', 'E-H', 'E-B', 'E-D', 'E-F', 'E-J', 'E-K'],
         'location_F': ['F-F', 'F-H', 'F-D', 'F-A', 'F-L', 'F-K', 'F-M', 'F-B', 'F-E', 'F-A', 'F-C', 'F-J', 'F-I'],
         'location_G': ['G-G', 'G-E', 'G-C', 'G-A', 'G-M', 'G-I', 'G-L', 'G-H', 'G-F', 'G-D', 'G-B', 'G-J', 'G-K'],
         'location_H': ['H-H', 'H-F', 'H-D', 'H-A', 'H-L', 'H-K', 'H-M', 'H-B', 'H-E', 'H-C', 'H-A', 'H-J', 'H-I'],
         'location_bid': ['', '', '', '', '50', '40', '30', '', '', '', '', '20', '10']
         }
    ca_locations_dist_df = pd.DataFrame(data=d)
    return ca_locations_dist_df


def get_tr_list(slot, br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name) of active tr agents"""
    # agent_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    # agents_df = agents_data()
    agents_df  = globals.agnts_full
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no s       si deber      a cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "tc"]
    br_data_df = br_data_df.reset_index(drop=True)
    to = str()
    if slot == 1:
        ca_location_1 = agent_df.loc[0, 'location_1']
        br_data_df['location_ca'] = str(ca_location_1)  ### location 1!!!!
        br_data_df['dash'] = "-"
        br_data_df["from_to"] = br_data_df["location_ca"] + br_data_df["dash"] + br_data_df["location"]
        to = "location_" + ca_location_1  # location 1!!!!!
    elif slot == 2:
        ca_location_2 = agent_df.loc[0, 'location_2']
        br_data_df['location_ca'] = str(ca_location_2)  ### location 2!!!!
        br_data_df['dash'] = "-"
        br_data_df["from_to"] = br_data_df["location_ca"] + br_data_df["dash"] + br_data_df["location"]
        to = "location_" + ca_location_2  # location 2!!!!!
    active_users_location_df = br_data_df
    ca_locations_dist_df = locations_min_distances()
    ca_locations_dist_df = ca_locations_dist_df[['id_min', to]]
    tr_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in tr_list:
        a = ca_locations_dist_df.loc[ca_locations_dist_df[to] == i]
        id_loop = a.loc[a.index[-1], 'id_min']
        tr_to_loop = a.loc[a.index[-1], to]
        keys.append(id_loop)
        values.append(tr_to_loop)
    segment = dict(zip(keys, values))
    segment_df = pd.DataFrame([segment])
    segment_df = segment_df.T
    indexes = segment_df.index.values.tolist()
    segment_df = segment_df.rename(columns={0: "segment"})
    segment_df.insert(loc=0, column='id_min', value=indexes)
    segment_df = segment_df.sort_values(by=['id_min'])
    segment_df = segment_df.reset_index(drop=True)
    # segment_df contains the location of active tr and id_name
    # sorted by shortest distance to them
    tr_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in tr_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(columns={'from_to': 'segment'})
    results = active_users_location_df.merge(segment_df, on='segment')

    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def get_wh_list(br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name) of active wh agents"""
    agents_df = globals.agnts_full
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no s       si deber      a cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "wh"]
    br_data_df = br_data_df.reset_index(drop=True)
    to = str()
    ca_location_2 = agent_df.loc[0, 'location_2']
    br_data_df['location_ca'] = str(ca_location_2)  ### location 2!!!!
    br_data_df['dash'] = "-"
    br_data_df["from_to"] = br_data_df["location_ca"] + br_data_df["dash"] + br_data_df["location"]
    to = "location_" + ca_location_2  # location 2!!!!!
    active_users_location_df = br_data_df
    ca_locations_dist_df = locations_min_distances()
    ca_locations_dist_df = ca_locations_dist_df[['id_min', to]]
    wh_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in wh_list:
        a = ca_locations_dist_df.loc[ca_locations_dist_df[to] == i]
        id_loop = a.loc[a.index[-1], 'id_min']
        tr_to_loop = a.loc[a.index[-1], to]
        keys.append(id_loop)
        values.append(tr_to_loop)
    segment = dict(zip(keys, values))
    segment_df = pd.DataFrame([segment])
    segment_df = segment_df.T
    indexes = segment_df.index.values.tolist()
    segment_df = segment_df.rename(columns={0: "segment"})
    segment_df.insert(loc=0, column='id_min', value=indexes)
    segment_df = segment_df.sort_values(by=['id_min'])
    segment_df = segment_df.reset_index(drop=True)
    # segment_df contains the location of active tr and id_name
    # sorted by shortest distance to them
    tr_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in tr_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(columns={'from_to': 'segment'})
    results = active_users_location_df.merge(segment_df, on='segment')
    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def get_coil_list(br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name)
    of active coil agents. Coils are sorted by distance"""
    agents_df = globals.agnts_full
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no s       si deber      a cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "coil"]
    to = str()
    ca_location_1 = agent_df.loc[0, 'location_1']
    br_data_df['location_ca'] = str(ca_location_1)  ### location 1!!!!
    br_data_df['dash'] = "-"
    br_data_df["from_to"] = br_data_df["location_ca"] + \
                br_data_df["dash"] + br_data_df["location"]
    to = "location_" + ca_location_1  # location 1!!!!!
    active_users_location_df = br_data_df
    ca_locations_dist_df = locations_min_distances()
    ca_locations_dist_df = ca_locations_dist_df[['id_min', to]]
    tr_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in tr_list:
        a = ca_locations_dist_df.loc[ca_locations_dist_df[to] == i]
        id_loop = a.loc[a.index[-1], 'id_min']
        tr_to_loop = a.loc[a.index[-1], to]
        keys.append(id_loop)
        values.append(tr_to_loop)
    segment = dict(zip(keys, values))
    segment_df = pd.DataFrame([segment])
    segment_df = segment_df.T
    indexes = segment_df.index.values.tolist()
    segment_df = segment_df.rename(columns={0: "segment"})
    segment_df.insert(loc=0, column='id_min', value=indexes)
    segment_df = segment_df.sort_values(by=['id_min'])
    segment_df = segment_df.reset_index(drop=True)
    # segment_df contains the location of active tr and
    # id_name sorted by shortest distance to them
    tr_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in tr_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(\
                    columns={'from_to': 'segment'})
    results = active_users_location_df.merge(segment_df, on='segment')
    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def ca_msg_to(msg_body):
    """Returns msg object without destination"""
    msg_tr = Message()
    msg_tr.body = msg_body
    msg_tr.set_metadata("performative", "inform")
    return msg_tr


def br_msg_to(msg_body):
    """Returns msg object without destination"""
    msg = Message()
    msg.body = msg_body
    msg.set_metadata("performative", "inform")
    return msg


def br_int_fab_df(agent_df):
    """Returns df to send to interrupted fab coil"""
    agent_df.at[0, 'int_fab'] = 1
    return agent_df


def br_get_requested_df(agent_name, *args):
    """Returns a df in which calculations can be done"""
    df = pd.DataFrame()
    if args == "coils":
        search_str = '{"id":{"0":"' + "coil" + '_'  # tiene que encontrar todas las coil que quieran fabricarse y como mucho los últimos 1000 registros.
    else:
        search_str = "activation_time"  # takes every record with this. Each agent is sending that info while alive communicating to log.
    l = []
    N = 1000
    with open(r"log.log") as f:
        for line in f.readlines()[-N:]:  # from the last 1000 lines
            if search_str in line:  # find search_str
                n = line.find("{")
                a = line[n:]
                l.append(a)
    df_0 = pd.DataFrame(l, columns=['register'])
    for ind in df_0.index:
        if ind == 0:
            element = df_0.loc[ind, 'register']
            z = json.loads(element)
            df = pd.DataFrame.from_dict(z)
        else:
            element = df_0.loc[ind, 'register']
            y = json.loads(element)
            b = pd.DataFrame.from_dict(y)
            df = df.append(b)
    df = df.reset_index(drop=True)
    if args == "coils":  # if ca is requesting
        df = df.loc[0, 'to_do'] == "search_auction"  # filters coils searching for auction
    return df


def check_active_users_loc_times(agent_name, *args):
    """Returns a json with tr & ca averages operation time"""
    if args == "coils":
        df = br_get_requested_df(agent_name, args)
    else:
        df = br_get_requested_df(agent_name)
    # Calculate means
    df['time'] = pd.to_datetime(df['time'])
    df['AVG(tr_op_time)'] = pd.to_datetime(df['AVG(tr_op_time)'], unit='ms')
    df['AVG(ca_op_time)'] = pd.to_datetime(df['AVG(ca_op_time)'], unit='ms')
    tr_avg = df['AVG(tr_op_time)'].mean()  # avg(operation_time_tr)
    ca_avg = df['AVG(ca_op_time)'].mean()  # avg(operation_time_ca)
    if pd.isnull(tr_avg):
        if pd.isnull(ca_avg):
            tr_avg = 3.5
            ca_avg = 9
    else:
        tr_avg = tr_avg - datetime.datetime(1970, 1, 1)
        ca_avg = ca_avg - datetime.datetime(1970, 1, 1)
        tr_avg = tr_avg.total_seconds() / 60
        ca_avg = ca_avg.total_seconds() / 60
    op_times_df = pd.DataFrame([], columns=['AVG(tr_op_time)', 'AVG(ca_op_time)'])
    op_times_df.at[0, 'AVG(tr_op_time)'] = tr_avg
    op_times_df.at[0, 'AVG(ca_op_time)'] = ca_avg
    # Check active users locations
    sorted_df = df.sort_values(by=['time'])
    sorted_df = sorted_df.loc[sorted_df['status'] == "on"]
    active_time = datetime.datetime.now() - datetime.timedelta(seconds=300)
    sorted_df = sorted_df.loc[sorted_df['time'] < active_time]
    uniques = sorted_df['id']
    uniques = uniques.drop_duplicates()
    uniques = uniques.tolist()
    values = []
    keys = []
    for i in uniques:
        a = sorted_df.loc[sorted_df['id'] == i]
        last_id = a.loc[a.index[-1], 'id']
        last_location = a.loc[a.index[-1], 'location']
        keys.append(last_id)
        values.append(last_location)
    users_location = dict(zip(keys, values))
    users_location_df = pd.DataFrame([users_location])
    users_location_df = users_location_df.T
    indexes = users_location_df.index.values.tolist()
    users_location_df.insert(loc=0, column='agent', value=indexes)
    users_location_df = users_location_df.rename(columns={0: "location"})
    users_location_df = users_location_df.reset_index(drop=True)
    for i in range(len(users_location_df['agent'])):
        slice = users_location_df.loc[i, 'agent'][:-3]
        if slice == 'coil_':
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent'][:-4]
        elif slice == 'brow':
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent']
        else:
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent'][:-3]
    # Joins information
    users_location_df = users_location_df.join(op_times_df)
    users_location_json = users_location_df.to_json()
    return users_location_json


def confirm_tr_bookings_to_log(ca_to_tr_df, agent_directory, closest_tr_df, tr_assigned):
    """Builds the json to send to log after confirmed booking"""
    # agents_df = agents_data()
    agents_df = globals.agnts_full
    ca_to_log_df = ca_to_tr_df
    ca_to_log_df = ca_to_log_df.loc[:, 'id':'slot_2_end']
    ca_to_log_df["active_tr"] = ""
    ca_to_log_df.at[0, 'active_tr'] = closest_tr_df["Name"].tolist()
    ca_to_log_df.at[0, 'purpose'] = "inform"
    ca_to_log_df.at[0, 'request_type'] = ""
    ca_to_log_df.at[0, 'tr_slot_1'] = tr_assigned[0]
    ca_to_log_df.at[0, 'tr_slot_2'] = tr_assigned[1]
    this_time = datetime.datetime.now()
    ca_to_log_df.at[0, 'time'] = this_time
    agents_df = agents_df.rename(columns={'User name': 'tr_slot_1'})
    agents_df.drop(['Location', 'Capacity'], axis=1, inplace=True)
    ca_to_log_df = ca_to_log_df.merge(agents_df, on='tr_slot_1')
    ca_to_log_df = ca_to_log_df.rename(columns={'Name': 'name_tr_slot_1'})
    agents_df = agents_df.rename(columns={'tr_slot_1': 'tr_slot_2'})
    ca_to_log_df = ca_to_log_df.merge(agents_df, on='tr_slot_2')
    ca_to_log_df = ca_to_log_df.rename(columns={'Name': 'name_tr_slot_2'})
    ca_to_log_df.drop(['action', 'Location1_y', 'Location2_y', 'Location1_x', \
                       'Location2_x'], axis=1, inplace=True)
    ca_to_log_df = ca_to_log_df.rename(columns={'tr_slot_1': 'jid_tr_slot_1'})
    ca_to_log_df = ca_to_log_df.rename(columns={'tr_slot_2': 'jid_tr_slot_2'})
    return ca_to_log_df.to_json()

def confirm_wh_booking_to_log(ca_to_wh_df, wh_assigned, agent_directory, closest_wh_df):
    # agents_df = agents_data()
    agents_df = globals.agnts_full
    this_time = datetime.datetime.now()
    ca_to_log_df = ca_to_wh_df
    ca_to_log_df.at[0, 'purpose'] = "inform"
    ca_to_log_df.at[0, 'request_type'] = ""
    ca_to_log_df.at[0, 'time'] = this_time
    wh_assigned_str = ""
    wh_assigned_str = wh_assigned_str.join(map(str, wh_assigned))
    ca_to_log_df['jid_wh'] = ""
    ca_to_log_df['name_wh'] = ""
    ca_to_log_df['active_wh'] = ""
    ca_to_log_df.at[0, 'active_wh'] = closest_wh_df["Name"].tolist()
    ca_to_log_df.at[0, 'jid_wh'] = wh_assigned_str
    agents_df.drop(['Location', 'Capacity'], axis=1, inplace=True)
    agents_df = agents_df.loc[agents_df['User name'] == wh_assigned_str]
    agents_df = agents_df.reset_index(drop=True)
    ca_to_log_df['name_wh'] = ""
    ca_to_log_df.at[0, 'name_wh'] = agents_df.loc[0, 'Name']
    return ca_to_log_df.to_json()


def plc_temp(coil_df):
    """Once auction is completed, temperatures of the oven are saved for next processing. It returns the information as df
    """


def ca_auction_df():
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                        'location_2', 'location', 'purpose', 'request_type', \
                        'time', 'activation_time', 'T1', 'T2', 'T3', 'T4', 'T5', 'q'])


def ca_assigned_auction_df():
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', \
                        'location_2', 'location', 'purpose', 'request_type', \
                        'time', 'activation_time', 'T1', 'T2', 'T3', 'T4', \
                        'T5', 'q', 'bid_status', 'bid', ])

def ca_to_coils_initial_df(agent_df, plc_temp_df):
    """Builds df to send to coils with auction information made by agent_df and last plc temperatures"""
    agent_df.at[0, 'T1'] = plc_temp_df.loc[0, 'T1']
    agent_df.at[0, 'T2'] = plc_temp_df.loc[0, 'T2']
    agent_df.at[0, 'T3'] = plc_temp_df.loc[0, 'T3']
    agent_df.at[0, 'T4'] = plc_temp_df.loc[0, 'T4']
    agent_df.at[0, 'T5'] = plc_temp_df.loc[0, 'T5']
    return agent_df

def ca_to_coils_second_df(agent_df):
    """Builds df to send to coils with counterbid information made by agent_df"""


def coil_enter_auction_rating(auction_agent_df, agent_df, not_enterted_auctions):
    """Gives rating to auctionable resource. Returns 1 if positive, 0 if negative"""
    auction_agent_df = auction_agent_df[['T1', 'T2', 'T3', 'T4', 'T5']]
    a = auction_agent_df.isnull().sum().sum()
    if int(not_enterted_auctions) > int(6):
        # in case this coil did not enter auctions for more than 20 times, it will enter next one.
        answer = 1  # positive
    elif a == 0: # evaluates if ca does not have Temps, which means it is the first auction.
        temp_dif_T1 = abs(auction_agent_df.loc[0, 'T1'] - agent_df.loc[0, 'T1'])
        temp_dif_T2 = abs(auction_agent_df.loc[0, 'T2'] - agent_df.loc[0, 'T2'])
        temp_dif_T3 = abs(auction_agent_df.loc[0, 'T3'] - agent_df.loc[0, 'T3'])
        temp_dif_T4 = abs(auction_agent_df.loc[0, 'T4'] - agent_df.loc[0, 'T4'])
        temp_dif_T5 = abs(auction_agent_df.loc[0, 'T5'] - agent_df.loc[0, 'T5'])
        temp_dif_list = [temp_dif_T1, temp_dif_T2, temp_dif_T3, temp_dif_T4, temp_dif_T5]
        max_temp_diff = 80
        # manually set up, this is a parameter that the process engineer should decide. It can be passed to args.
        for i in temp_dif_list:
            if i > max_temp_diff:
                answer = 0  # negative
            else:
                answer = 1  # positive
        total_temp_dif = temp_dif_T1 + temp_dif_T2 + temp_dif_T3 + temp_dif_T4 + temp_dif_T5
        if total_temp_dif > 200:
            # extra constraint on overall difference. Objective is that
            # auctionable coils have similiar paramters to current
            answer = 0  # negative
        else:
            answer = 1  # positive
    else:
        answer = 1  # positive ofr being first auction
    return answer


def location_bid(ca_agent_df, agent_df):
    """Returns location bid as function of distance from coil wh to resource.
    50 token to nearest ca resource, 40, 30, 20, 10 to less close resource"""
    #
    locations_min_distances_df = locations_min_distances()
    ca_location_1 = ca_agent_df.loc[0, 'location_1']
    to = str()
    to = "location_" + ca_location_1
    locations_min_distances_df = locations_min_distances_df[[to, 'location_bid']]
    locations_min_distances_df = locations_min_distances_df.rename(\
                columns={to: 'segment'})
    closest_coils_df = locations_min_distances_df[['segment']]
    df = closest_coils_df.merge(locations_min_distances_df, on='segment')
    coil_location = agent_df.loc[0, 'location']
    segment = ca_location_1 + '-' + coil_location
    df1 = df.loc[df['segment'] == segment]
    df1 = df1.reset_index(drop=True)
    location_bid_ = df1.loc[0, 'location_bid']
    return int(location_bid_)


def coil_bid(ca_agent_df, agent_df, agent_status_var):
    """Creates bid or counterbid"""
    budget = agent_df.loc[0, 'budget']
    loc_bid = location_bid(ca_agent_df, agent_df)
    auction_level_bid = ""
    int_fab_bid = ""
    a = ca_agent_df.loc[0, 'auction_level']
    if agent_status_var == "auction":
        if ca_agent_df.loc[0, 'auction_level'] == 1:
            auction_level_bid = 0.15 * budget
            # extra 15% if it is in final state of auction
        elif ca_agent_df.loc[0, 'auction_level'] == 2:
            auction_level_bid = 0.3 * budget
            # extra 15% if it is in final state of auction
        elif ca_agent_df.loc[0, 'auction_level'] == 3:
            #this does no make sense but a bug appears when it
            # receives the message where it shoudnt.
            auction_level_bid = 0.3 * budget
            # extra 15% if it is in final state of auction
        if agent_df.loc[0, 'int_fab'] == 1:
            int_fab_bid = 0.15 * budget
        else:
            int_fab_bid = 0
    co_bid = int(loc_bid) + int(auction_level_bid) + int(int_fab_bid)
    return co_bid

def production_cost(configuracion_df,coil_df, i):
    z = coil_df.loc[i,'ancho'] - configuracion_df.loc[0,'coil_width']
    m = coil_df.loc[i,'largo'] - configuracion_df.loc[0,'coil_length']
    n = coil_df.loc[i,'espesor'] - configuracion_df.loc[0,'coil_thickness']
    cost = float(z * 4 + m * 0.05 + n * 2)
    return cost

def transport_cost(to):
    costes_df = pd.DataFrame()
    costes_df['From'] = ['NWW1', 'NWW1', 'NWW1','NWW1','NWW1','NWW3','NWW3','NWW3','NWW3','NWW3','NWW4','NWW4','NWW4','NWW4','NWW4']
    costes_df['CrossTransport'] = [24.6, 24.6, 0, 0, 55.6, 74.8, 74.8, 50.2, 50.2, 32.3, 71.5, 71.5, 46.9,46.9, 0]
    costes_df['Supply'] = [24.6, 24.6, 21.1, 21.1, 5.7, 24.6, 24.6, 21.1, 21.1, 5.7, 24.6, 24.6, 21.1, 21.1, 5.7]
    costes_df['To'] = ['va08', 'va09', 'va10','va11','va12','va08','va09','va10','va11','va12','va08','va09','va10','va11','va12']
    costes_df = costes_df.loc[costes_df['To'] == to]
    costes_df = costes_df.reset_index(drop=True)
    return costes_df


def va_bid_evaluation(coil_msgs_df, va_data_df, step):
    key = []
    transport_cost_df = transport_cost(va_data_df.loc[0,'id'].split('@')[0])
    for i in range(transport_cost_df.shape[0]):
        m = transport_cost_df.loc[i, 'CrossTransport']
        n = transport_cost_df.loc[i, 'Supply']
        key.append(n+m)
    transport_cost_df['transport_cost'] = key
    transport_cost_df = transport_cost_df.loc[:, ['From', 'To', 'transport_cost']]
    for i in range(coil_msgs_df.shape[0]):
        coil_msgs_df.at[i, 'production_cost'] = production_cost(va_data_df, coil_msgs_df, i)
    #
    coil_msgs_df = coil_msgs_df.merge(transport_cost_df, on='From', sort=False)
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'production_cost']
        n = coil_msgs_df.loc[i, 'transport_cost']
        coil_msgs_df.loc[i, 'minimum_price'] = m + n
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'minimum_price']
        if step == 'counterbid':
            n = coil_msgs_df.loc[i, 'counterbid']
            coil_msgs_df.loc[i, 'profit'] = n - m
        n = coil_msgs_df.loc[i, 'bid']
        coil_msgs_df.loc[i, 'difference'] = m - n
    if step == 'bid':
        results = coil_msgs_df[['agent_type', 'id', 'coil_jid', 'bid', \
                            'minimum_price','difference', 'ancho', 'largo',\
                            'espesor', 'ship_date','budget_remaining']]
        results = results.sort_values(by=['difference'])
    else: # 'counterbid'
        results = coil_msgs_df[['agent_type', 'id', 'coil_jid', 'bid', \
                            'minimum_price', 'ancho', 'largo','difference',\
                            'espesor', 'ship_date','budget_remaining',\
                            'counterbid','profit','User_name_va']]
        results = results.sort_values(by=['profit'], ascending = False)
    #
    results = results.reset_index(drop=True)
    value = []
    for i in range(results.shape[0]):
        value.append(i+1)
    results.insert(loc=0, column='position', value=value)
    return results

def va_result(coil_ofertas_df, jid_list,step):
    df = pd.DataFrame([], columns=['Coil', 'Minimum_price', 'Bid',\
                                   'Difference', 'Budget_remaining'])
    if step == 'counterbid' :
        df = pd.DataFrame([], columns=['Coil', 'Minimum_price', 'Bid',\
                                   'Difference', 'Budget_remaining',\
                                   'Counterbid','Profit'])
    #
    for i in range(len(jid_list)):
        df.at[i, 'Coil'] = coil_ofertas_df.loc[i, 'id']
        df.at[i, 'Minimum_price'] = coil_ofertas_df.loc[i, 'minimum_price']
        df.at[i, 'Bid'] = coil_ofertas_df.loc[i, 'bid']
        df.at[i, 'Difference'] = coil_ofertas_df.loc[i, 'difference']
        df.at[i, 'Budget_remaining'] = coil_ofertas_df.loc[i, 'budget_remaining']
        if step == 'counterbid' :
            df.at[i, 'Counterbid'] = coil_ofertas_df.loc[i, 'counterbid']
            df.at[i, 'Profit'] = coil_ofertas_df.loc[i, 'profit']
    return df

def auction_bid_evaluation(coil_msgs_df, agent_df):
    """Evaluates coils and their bids and returns a df with an extra column with rating to coils proposal"""
    ev_df = coil_msgs_df[['id', 'agent_type', 'location', 'int_fab', 'bid', \
                          'bid_status', 'coil_length', 'coil_width', \
                          'coil_thickness', 'coil_weight', 'setup_speed', \
                          'budget', 'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'ship_date']]
    ev_df = ev_df.reset_index(drop=True)
    # Ship_date evaluation. Extra column with ship date rating
    sd_ev_df = coil_msgs_df[['id', 'agent_type', 'location', 'int_fab', 'bid', \
                             'bid_status', 'budget', 'ship_date']]
    sd_ev_df = sd_ev_df.reindex(columns=['id', 'agent_type', 'location', \
                    'int_fab', 'bid', 'bid_status','coil_length', 'coil_width', \
                    'coil_thickness', 'coil_weight', 'setup_speed', 'budget', \
                    'ship_date', 'ship_date_seconds', 'ship_date_rating'])
    sd_ev_df['ship_date'] = pd.to_datetime(sd_ev_df['ship_date']) #, unit='ms'
    sd_ev_df = sd_ev_df.reset_index(drop=True)
    for i in range(len(sd_ev_df['ship_date'].tolist())):
        date = sd_ev_df.loc[i, 'ship_date'].timestamp()
        sd_ev_df.at[i, 'ship_date_seconds'] = date
    sd_ev_df = sd_ev_df.sort_values(by=['ship_date_seconds'])
    sd_ev_df = sd_ev_df.reset_index(drop=True)
    ship_date_list = sd_ev_df['ship_date_seconds'].tolist()
    max_date = max(ship_date_list)
    min_date = min(ship_date_list)
    max_weight = 40  # The ship_date weights 40 out of 100 on the rating.
    rating_list = []
    for i in ship_date_list:
        rating = linear_ec(max_weight, max_date, min_date, i)
        """y1 is the max weight, y2 is the min weight = 0. x2
        is the farest date, x1 is the closest date"""
        rating_list.append(rating)
    for i in range(len(rating_list)):
        sd_ev_df.at[i, 'ship_date_rating'] = rating_list[i]
        # sd_ev_df has ship_rating info
    ev_df = sd_ev_df
    # ev_df will contain all the final evaluation. added ship_date rating
    # Temp evaluation. Extra column with temp match rating
    t_ev_df = coil_msgs_df[['id', 'agent_type', 'location', 'int_fab', 'bid', \
                    'bid_status', 'budget', 'T1', 'T2', 'T3', 'T4', 'T5', 'q']]
    current_t_df = agent_df[['id', 'agent_type', 'location_1', 'bid_status', \
                    'T1', 'T2', 'T3', 'T4', 'T5', 'q']]
    t_ev_df = t_ev_df.reindex(columns=['id', 'agent_type', 'location_1', 'bid', \
                    'bid_status', 'budget', 'T1', 'T2', 'T3', 'T4', 'T5', 'q', \
                    'T1dif', 'T2dif', 'T3dif', 'T4dif', 'T5dif', \
                    'total_temp_dif', 'temp_rating'])
    t_ev_df = t_ev_df.reset_index(drop=True)
    for i in range(len(t_ev_df['T1'].tolist())):
        temp_dif_T1 = abs(t_ev_df.loc[i, 'T1'] - current_t_df.loc[0, 'T1'])
        temp_dif_T2 = abs(t_ev_df.loc[i, 'T2'] - current_t_df.loc[0, 'T2'])
        temp_dif_T3 = abs(t_ev_df.loc[i, 'T3'] - current_t_df.loc[0, 'T3'])
        temp_dif_T4 = abs(t_ev_df.loc[i, 'T4'] - current_t_df.loc[0, 'T4'])
        temp_dif_T5 = abs(t_ev_df.loc[i, 'T5'] - current_t_df.loc[0, 'T5'])
        temp_dif_list = [temp_dif_T1, temp_dif_T2, temp_dif_T3, temp_dif_T4, temp_dif_T5]
        t_ev_df.at[i, 'T1dif'] = temp_dif_T1
        t_ev_df.at[i, 'T2dif'] = temp_dif_T2
        t_ev_df.at[i, 'T3dif'] = temp_dif_T3
        t_ev_df.at[i, 'T4dif'] = temp_dif_T4
        t_ev_df.at[i, 'T5dif'] = temp_dif_T5
        total_temp_dif = temp_dif_T1 + temp_dif_T2 + temp_dif_T3 + temp_dif_T4 + temp_dif_T5
        t_ev_df.at[i, 'total_temp_dif'] = total_temp_dif
    temp_list = t_ev_df['total_temp_dif'].tolist()
    max_temp_dif = max(temp_list)
    min_temp_dif = min(temp_list)
    max_weight = 30  # The temp difference weights 30 out of 100 on the rating.
    rating_list = []
    for i in temp_list:
        rating = linear_ec(max_weight, max_temp_dif, min_temp_dif, i)
        """y1 is the max weight, y2 is the min weight = 0. x2 is the
        max temp dif, x1 is the min temp dif"""
        rating_list.append(rating)
    for i in range(len(rating_list)):
        t_ev_df.at[i, 'temp_rating'] = rating_list[i]  # t_ev_df has temp_rating info
    to_merge_df = t_ev_df[['id', 'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'T1dif', 'T2dif', \
                           'T3dif', 'T4dif', 'T5dif', 'total_temp_dif', 'temp_rating']]
    ev_df = ev_df.merge(to_merge_df, on='id')  # added temp maatching rating
    # Bids evaluation. Extra column with bids rating
    bids_ev_df = coil_msgs_df[['id', 'agent_type', 'location', 'int_fab', 'bid', \
                            'bid_status', 'budget']]
    bids_ev_df = bids_ev_df.reindex(columns=['id', 'agent_type', 'location', \
                            'int_fab', 'bid', 'bid_status', 'budget', 'bid_rating'])
    bids_ev_df = bids_ev_df.reset_index(drop=True)
    bids_list = bids_ev_df['bid'].tolist()
    max_bid = max(bids_list)
    min_bid = min(bids_list)
    max_weight = 20  # The bid weights 20 out of 100 on the rating.
    rating_list = []
    for i in bids_list:
        rating = linear_ec(max_weight, min_bid, max_bid, i)
        """y1 is the max weight, y2 is the min weight = 0.
        x2 is the min_bid, x1 is the max_bid"""
        rating_list.append(rating)
    for i in range(len(rating_list)):
        bids_ev_df.at[i, 'bid_rating'] = rating_list[i]
        # bids_ev_df has bids_rating info
    to_merge_df = bids_ev_df[['id', 'bid_rating']]
    ev_df = ev_df.merge(to_merge_df, on='id')  # added bid rating
    # Interrupted_fab evaluation
    intfab_ev_df = coil_msgs_df[['id', 'agent_type', 'location', 'int_fab', 'bid', \
                    'bid_status', 'budget']]
    intfab_ev_df = intfab_ev_df.reindex(columns=['id', 'agent_type', 'location', \
                    'int_fab', 'bid', 'bid_status', 'budget', 'int_fab_priority', \
                    'int_fab_rating'])
    intfab_ev_df = intfab_ev_df.reset_index(drop=True)
    intfab_list = bids_ev_df['id'].tolist()
    for i in range(len(intfab_list)):
        if intfab_ev_df.at[i, 'int_fab'] == "yes":
            intfab_ev_df.at[i, 'int_fab_priority'] = 0.15 * intfab_ev_df.at[i, \
                        'budget']
            # extra 15 tokens if the coil was previously interrupted in fabrication
        else:
            intfab_ev_df.at[i, 'int_fab_priority'] = 0
    intfabpriority_list = intfab_ev_df['int_fab_priority'].tolist()
    max_int_fab_priority = max(intfabpriority_list)
    min_int_fab_priority = min(intfabpriority_list)
    max_weight = 15  # The interrupted fab weights 15 out of 100 on the rating.
    rating_list = []
    for i in intfabpriority_list:
        rating = linear_ec(max_weight, min_int_fab_priority, max_int_fab_priority, i)
        """y1 is the max weight, y2 is the min weight = 0.
        x2 is the min_int_fab_priotity, x1 is the max_int_fab_priotity"""
        rating_list.append(rating)
    for i in range(len(rating_list)):
        if len(rating_list) == 1:
            intfab_ev_df.at[i, 'int_fab_rating'] = 0.0
        else:
            intfab_ev_df.at[i, 'int_fab_rating'] = rating_list[i]
    to_merge_df = intfab_ev_df[['id', 'int_fab_priority', 'int_fab_rating']]
    ev_df = ev_df.merge(to_merge_df, on='id')  # added interrupted fab rating
    #sum all and provide final rating.

    ev_df = ev_df.reindex(
        columns=['id', 'agent_type', 'location', 'int_fab', 'bid', 'bid_status', \
                 'budget', 'ship_date', 'ship_date_seconds', 'ship_date_rating', \
                 'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'T1dif', 'T2dif', 'T3dif', \
                 'T4dif', 'T5dif','total_temp_dif','temp_rating', 'bid_rating', \
                 'int_fab_priority', 'int_fab_rating', 'rating', 'rating_dif', \
                 'negotiation'])
    ev_df['rating'] = ev_df['ship_date_rating'] + ev_df['temp_rating'] + ev_df[\
                 'bid_rating'] + ev_df['int_fab_rating']
    ev_df = ev_df.sort_values(by=['rating'], ascending=False)
    ev_df = ev_df.reset_index(drop=True)
    negotiation_limit = 10
    for i in range(len(ev_df['rating'].tolist())):
        ev_df.at[i, 'rating_dif'] = ev_df.loc[0, 'rating'] - ev_df.loc[i, 'rating']
        if ev_df.loc[i, 'rating_dif'] <= 10:
            ev_df.at[i, 'negotiation'] = 1
        else:
            ev_df.at[i, 'negotiation'] = 0
    return ev_df


def linear_ec(y1, x2, x1, x):
    """inputs 2 points and another x point.
    Returns the value y as function of x: y=f(x)
    y1 is the max weight, y2 is the min weight = 0.
    x1 is the closest date, x2 is the farest date"""
    y2 = 0
    if x2-x1 == 0: # this is the case when there is only one coil.
        y = y1
    else:
        m = (y2-y1)/(x2-x1)
        y = (m*(x-x2))-y2
    if y == -0:
        y = 0
    return y


def ca_negotiate(evaluation_df, coil_msgs_df):
    """Returns a df with coils to send message asking to counterbid"""
    negotiate_list = []
    for i in range(len(evaluation_df['rating'].tolist())):
        if evaluation_df.loc[i, 'negotiation'] == 1:
            negotiate_list.append(evaluation_df.loc[i, 'id'])
    df = pd.DataFrame()
    for i in negotiate_list:
        df0 = coil_msgs_df.loc[coil_msgs_df['id'] == i]
        df = df.append(df0)
        df = df.reset_index(drop=True)
    return df

def auction_va_kpis(agent_df, coil_msgs_df, auction_df, process_df,\
                 ca_counter_bid_df, *args):
    """Creates a df with all auction information"""
    df = auction_blank_df()
    df1 = pd.DataFrame()
    if args:
        winner = df1.loc[0, 'id']
    else:
        winner = coil_msgs_df.loc[0, 'id']
    #ca_counter_bid_df = ca_counter_bid_df.loc[ca_counter_bid_df['id'] == winner]
    #ca agent info
    df.at[0, 'id'] = agent_df.loc[0, 'id']
    df.at[0, 'agent_type'] = agent_df.loc[0, 'agent_type'].upper()
    df.at[0, 'location_1'] = coil_msgs_df.loc[0, 'From']
    df.at[0, 'location_2'] = 'END'
    df.at[0, 'location'] = (agent_df.loc[0, 'id'].split('@')[0]).upper()
    # winner coil info
    df.at[0, 'coil_auction_winner'] = coil_msgs_df.loc[0,'id']
    df.at[0, 'auction_number'] = auction_df.loc[0, 'number_auction_completed']
    df.at[0, 'coil_location_1'] = coil_msgs_df.loc[0, 'From']
    df.at[0, 'coil_length'] = coil_msgs_df.loc[0, 'coil_length']
    df.at[0, 'coil_width'] = coil_msgs_df.loc[0, 'coil_width']
    df.at[0, 'coil_thickness'] = coil_msgs_df.loc[0, 'coil_thickness']
    df.at[0, 'coil_weight'] = df.at[0, 'coil_length']*df.at[0, 'coil_width']*\
              df.at[0, 'coil_thickness']*7.85/1000.
    df.at[0, 'int_fab'] = auction_df.loc[0, 'int_fab']
    df.at[0, 'bid'] = ca_counter_bid_df.loc[0, 'Bid']
    df.at[0, 'budget'] = coil_msgs_df.loc[0, 'budget']
    df.at[0, 'ship_date'] = coil_msgs_df.loc[0, 'ship_date']
    df.at[0, 'setup_speed'] = agent_df.loc[0, 'setup_speed']
    # df.at[0, 'T1'] = coil_msgs_df.loc[0, 'T1']
    # df.at[0, 'T2'] = coil_msgs_df.loc[0, 'T2']
    # df.at[0, 'T3'] = coil_msgs_df.loc[0, 'T3']
    # df.at[0, 'T4'] = coil_msgs_df.loc[0, 'T4']
    # df.at[0, 'T5'] = coil_msgs_df.loc[0, 'T5']
    # df.at[0, 'q'] = coil_msgs_df.loc[0, 'q']
    # df.at[0, 'T1dif'] = coil_msgs_df.loc[0, 'T1dif']
    # df.at[0, 'T2dif'] = coil_msgs_df.loc[0, 'T2dif']
    # df.at[0, 'T3dif'] = coil_msgs_df.loc[0, 'T3dif']
    # df.at[0, 'T4dif'] = coil_msgs_df.loc[0, 'T4dif']
    # df.at[0, 'T5dif'] = coil_msgs_df.loc[0, 'T5dif']
    # df.at[0, 'total_temp_dif'] = coil_msgs_df.loc[0, 'total_temp_dif']
    # df.at[0, 'temp_rating'] = coil_msgs_df.loc[0, 'temp_rating']
    df.at[0, 'bid_rating'] = ca_counter_bid_df.loc[0, 'Counterbid'] / \
                ca_counter_bid_df.loc[0, 'Bid']
    df.at[0, 'Profit']  = ca_counter_bid_df.loc[0, 'Profit']
    df.at[0, 'ship_date_rating'] = auction_df.loc[0, 'ship_date_rating']
    df.at[0, 'int_fab_priority'] = auction_df.loc[0, 'int_fab_priority']
    df.at[0, 'int_fab_rating'] = auction_df.loc[0, 'int_fab_rating']
    df.at[0, 'rating'] = auction_df.loc[0, 'rating']
    df.at[0, 'rating_dif'] = auction_df.loc[0, 'rating_dif']
    df.at[0, 'negotiation'] = auction_df.loc[0, 'negotiation']
    # auction info
    df.at[0, 'pre_auction_start'] = auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_start'] = auction_df.loc[0, 'auction_start']
    df.at[0, 'auction_finish'] = datetime.datetime.now()
    df.at[0, 'active_tr_slot_1'] = auction_df.loc[0, 'active_tr_slot_1']
    df.at[0, 'active_tr_slot_2'] = auction_df.loc[0, 'active_tr_slot_2']
    df.at[0, 'tr_booking_confirmation_at'] = auction_df.loc[0, 'tr_booking_confirmation_at']
    df.at[0, 'active_wh'] = auction_df.loc[0, 'active_wh']
    df.at[0, 'wh_booking_confirmation_at'] = auction_df.loc[0, 'wh_booking_confirmation_at']
    df.at[0, 'wh_location'] = auction_df.loc[0, 'wh_location']
    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'auction_coils'] = auction_df.loc[0, 'auction_coils']
    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'brAVG(tr_op_time)'] = auction_df.loc[0, 'brAVG(tr_op_time)']
    df.at[0, 'brAVG(ca_op_time)'] = auction_df.loc[0, 'brAVG(ca_op_time)']
    op_times_df = op_times(process_df, agent_df)
    df.at[0, 'fab_start'] = process_df['fab_start'].iloc[-1]
    df.at[0, 'fab_end'] = process_df['fab_end'].iloc[-1]
    df.at[0, 'AVG(tr_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[\
                                    0, 'AVG(tr_op_time)'])
    df.at[0, 'AVG(ca_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[\
                                    0, 'AVG(ca_op_time)'])
    # df.at[0, 'slot_1_start'] = auction_df.loc[0, 'slot_1_start']
    # df.at[0, 'slot_1_end'] = auction_df.loc[0, 'slot_1_end']
    # df.at[0, 'slot_2_start'] = auction_df.loc[0, 'slot_2_start']
    # df.at[0, 'slot_2_end'] = auction_df.loc[0, 'slot_2_end']
    # df.at[0, 'name_tr_slot_1'] = auction_df.loc[0, 'name_tr_slot_1']
    # df.at[0, 'name_tr_slot_2'] = auction_df.loc[0, 'name_tr_slot_2']
    # df.at[0, 'delivered_to_wh'] = auction_df.loc[0, 'delivered_to_wh']
    # df.at[0, 'handling_cost_slot_1'] = auction_df.loc[0, 'handling_cost_slot_1']
    # df.at[0, 'handling_cost_slot_2'] = auction_df.loc[0, 'handling_cost_slot_2']
    # df.at[0, 'coil_ratings_1'] = auction_df.loc[0, 'coil_ratings_1']
    # df.at[0, 'coil_ratings_2'] = auction_df.loc[0, 'coil_ratings_2']
    # df.at[0, 'pre_auction_duration'] = auction_df.loc[0, 'wh_booking_confirmation_at'] - \
    #                     auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_duration'] = df.loc[0, 'auction_finish'] - \
                        auction_df.loc[0, 'auction_start']
    gantt_df = gantt(df)
    df.at[0, 'gantt'] = gantt_df.to_dict()
    location_diagram_df = location_diagram(df)
    df.at[0, 'location_diagram'] = location_diagram_df.to_dict()
    return df

def location_diagram(auction_kpis_df):
    coord_dict = {
        'location': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M'],
        'x_coord': [10, 40, 10, 40, 10, 40, 10, 40, 5, 25, 45, 45, 5],
        'y_coord': [25, 25, 20, 20, 15, 15, 10, 10, 30, 40, 30, 5, 5]}
    coord_df = pd.DataFrame(data=coord_dict)
    resource = [auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id'], \
                auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id']]
    coil = [auction_kpis_df.loc[0, 'coil_auction_winner'], auction_kpis_df.loc[0, \
            'coil_auction_winner'], auction_kpis_df.loc[0, 'coil_auction_winner'],\
            auction_kpis_df.loc[0, 'coil_auction_winner']]
    loc_step = [1, 2, 3, 4]
    auction_id = ["", "", "", ""]
    location = [auction_kpis_df.loc[0, 'coil_location_1'], auction_kpis_df.loc[0, \
            'location_1'], auction_kpis_df.loc[0, 'location_2'], \
            auction_kpis_df.loc[0, 'wh_location']]
    df = pd.DataFrame([], columns=['resource', 'coil', 'loc_step', 'auction_id', \
            'location'])
    df['resource'] = resource
    df['coil'] = coil
    df['loc_step'] = loc_step
    df['auction_id'] = auction_id
    df['location'] = location
    results = df.merge(coord_df, on='location')
    return results


def gantt(auction_kpis_df):
    df = pd.DataFrame([], columns=['task_id', 'task_name', 'duration', 'start', \
            'resource', 'complete'])
    task_id = [1, 2, 3, 4, 5]
    task_name = ['pre_auction', 'auction', 'tr_slot1', 'processing', 'tr_slot2']
    duration = [auction_kpis_df.loc[0, 'pre_auction_duration'], auction_kpis_df.loc[\
            0, 'auction_duration'], auction_kpis_df.loc[0, 'slot_1_end'] - \
            auction_kpis_df.loc[0, 'slot_1_start'], auction_kpis_df.loc[0, \
            'AVG(ca_op_time)'], auction_kpis_df.loc[0, 'slot_2_end'] - \
            auction_kpis_df.loc[0, 'slot_2_start']]
    start = [auction_kpis_df.loc[0, 'pre_auction_start'], auction_kpis_df.loc[\
             0, 'auction_start'], auction_kpis_df.loc[0, 'slot_1_start'], \
             auction_kpis_df.loc[0, 'fab_start'], auction_kpis_df.loc[\
             0,'slot_2_start']]
    finish = [auction_kpis_df.loc[0, 'auction_start'], auction_kpis_df.loc[0, \
            'auction_finish'], auction_kpis_df.loc[0, 'slot_1_end'], \
             auction_kpis_df.loc[0, 'fab_end'], auction_kpis_df.loc[\
             0, 'slot_2_end']]
    resource = [auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id'], \
                auction_kpis_df.loc[0, 'name_tr_slot_1'], auction_kpis_df.loc[\
                0, 'id'], auction_kpis_df.loc[0, 'name_tr_slot_2']]
    complete = [100, 100, 100, 100, 100]
    df['task_id'] = task_id
    df['task_name'] = task_name
    df['duration'] = duration
    df['start'] = start
    df['finish'] = finish
    df['resource'] = resource
    df['complete'] = complete
    return df


def op_times(p_df, ca_data_df):
    df = ca_data_df
    df.at[0, 'AVG(ca_op_time)'] = p_df['processing_time'].iloc[-1]
    df.at[0, 'AVG(tr_op_time)'] = (3 + random()) * 60  # between 3 and 4
    return df


def get_agent_name(jid, agent_directory):
    agents_df = agents_data()
    df = agents_df.loc[agents_df['User name'] == jid]
    df = df.reset_index(drop=True)
    name = df.loc[0, 'Name']
    return name


def get_agent_location(agent_full_name):
    agents_df = agents_data()
    df = agents_df.loc[agents_df['Name'] == agent_full_name]
    df = df.reset_index(drop=True)
    location = df.loc[0, 'Location']
    return location


def set_process_df(df, ca_counter_bid_df, ca_to_tr_df):
    """Adds new line to process_df with new parameters"""
    if pd.isnull(df['fab_start'].iloc[-1]):
        new_line_df = pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, \
                    0.25, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],\
                    index=['fab_start', 'processing_time', 'start_auction_before', \
                           'start_next_auction_at', 'fab_end', 'setup_speed', \
                           'T1', 'T2', 'T3', 'T4', 'T5', 'q'])
        process_df = df.append(new_line_df, ignore_index=True)
        process_df = process_df.reset_index(drop=True)
        processing_time = (1/ca_counter_bid_df.loc[0, 'setup_speed']) * \
                          ca_counter_bid_df.loc[0, 'largo']
        process_df['start_auction_before'].iloc[-1] = 6.5 * 60
        start_auction_before = process_df['start_auction_before'].iloc[-1]
        process_df['processing_time'].iloc[-1] = processing_time
        # process_df['fab_start'].iloc[-1] = (ca_to_tr_df.loc[0, 'slot_1_end'] - \
        #                   datetime.timedelta(minutes=2.5))
        process_df['fab_start'].iloc[-1] = datetime.datetime.now() + \
                          datetime.timedelta(minutes=1.)
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + \
                          datetime.timedelta(seconds=processing_time)
        start_next_auction_at = process_df['fab_end'].iloc[-1] - \
                          datetime.timedelta(seconds=start_auction_before)
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
        a = process_df['fab_start'].iloc[-1]
    else:
        process_df.loc[process_df.index.max() + 1, 'start_auction_before'] = ""
        processing_time = (1/ca_counter_bid_df.loc[0, 'setup_speed']) * \
                          ca_counter_bid_df.loc[0, 'coil_length']
        process_df['processing_time'].iloc[-1] = processing_time
        process_df['fab_start'].iloc[-1] = process_df['fab_end'].iloc[-2]
        a = process_df['fab_start']
        process_df['fab_start'] = pd.to_datetime(process_df['fab_start'])
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + \
                          datetime.timedelta(seconds=processing_time)
        process_df['start_auction_before'].iloc[-1] = process_df[\
                          'start_auction_before'].iloc[-2]
        start_next_auction_at = process_df['fab_end'].iloc[-1] - \
                        datetime.timedelta(seconds=process_df[\
                        'start_auction_before'].iloc[-1])
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
    # process_df['T1'].iloc[-1] = ca_counter_bid_df.loc[0, 'T1']
    # process_df['T2'].iloc[-1] = ca_counter_bid_df.loc[0, 'T2']
    # process_df['T3'].iloc[-1] = ca_counter_bid_df.loc[0, 'T3']
    # process_df['T4'].iloc[-1] = ca_counter_bid_df.loc[0, 'T4']
    # process_df['T5'].iloc[-1] = ca_counter_bid_df.loc[0, 'T5']
    # process_df['q'].iloc[-1] = ca_counter_bid_df.loc[0, 'q']
    process_df['setup_speed'].iloc[-1] = ca_counter_bid_df.loc[0, 'setup_speed']
    return process_df


def modify_ca_data_df(p_df, ca_data_df):
    """modifies agent_df with current parameters"""
    ca_data_df.at[0, 'T1'] = p_df['T1'].iloc[-1]
    ca_data_df.at[0, 'T2'] = p_df['T2'].iloc[-1]
    ca_data_df.at[0, 'T3'] = p_df['T3'].iloc[-1]
    ca_data_df.at[0, 'T4'] = p_df['T4'].iloc[-1]
    ca_data_df.at[0, 'T5'] = p_df['T5'].iloc[-1]
    ca_data_df.at[0, 'q'] = p_df['q'].iloc[-1]
    ca_data_df.at[0, 'setup_speed'] = p_df['setup_speed'].iloc[-1]
    ca_data_df.loc[0, 'purpose'] = ''
    ca_data_df.loc[0, 'request_type'] = ''
    return ca_data_df


def get_agent_jid(agent_full_name, *args):
    agents_df = agents_data()
    df = agents_df.loc[agents_df['Name'] == agent_full_name]
    df = df.reset_index(drop=True)
    name = df.loc[0, 'User name']
    return name


def bid_register(agent_name, agent_full_name):
    """Creates bid register"""
    df = pd.DataFrame([], columns=['id','idres','agent_type','auction_dt',\
                    'initial_bid','second_bid','won_bid','accepted_bid',\
                    'Profit','seq'])
    #df.at[0, 'id'] = agent_full_name
    #df.at[0, 'agent_type'] = agent_name
    return df

def append_bid(bid, bid_register_df, agent_name, agent_full_name, ca_agent_df, bid_level, *args):
    """Appends bid and returns bid register"""
    """args: best_auction_agent_full_name"""
    df = pd.DataFrame([], columns=['id', 'agent_type', 'auction_owner', \
                        'initial_bid', 'second_bid', 'won_bid', 'accepted_bid'])
    df.at[0, 'id'] = agent_full_name
    df.at[0, 'agent_type'] = agent_name
    ca_agent_full_name = ca_agent_df.loc[0, 'id']
    df.at[0, 'auction_owner'] = ca_agent_full_name
    if bid_level == 'initial':
        df.at[0, 'initial_bid'] = bid
        bid_register_df = bid_register_df.append(df)
        bid_register_df = bid_register_df.reset_index(drop=True)
    elif bid_level == 'extrabid':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == ca_agent_full_name]
        bid_register_df.at[idx, 'second_bid'] = bid
    elif bid_level == 'acceptedbid':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == ca_agent_full_name]
        bid_register_df.at[idx, 'won_bid'] = 1
    elif bid_level == 'confirm':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == args]
        bid_register_df.at[idx, 'accepted_bid'] = 1
    return bid_register_df


def update_bid_va(bid_register_df,va_coil_msg_df):
    pj = bid_register_df.index[bid_register_df['id']==\
        va_coil_msg_df.loc[0,'Coil']]
    bid_register_df.loc[pj,'Bid'] = va_coil_msg_df.loc[0,'Bid']
    bid_register_df.loc[pj,'Minimum_price'] = va_coil_msg_df.loc[\
            0,'Minimum_price']
    bid_register_df.loc[pj,'Difference'] = va_coil_msg_df.loc[\
            0,'Difference']
    bid_register_df.loc[pj,'Budget_remaining'] = \
            va_coil_msg_df.loc[0,'Budget_remaining']
    bid_register_df.loc[pj,'Counterbid'] = \
            va_coil_msg_df.loc[0,'Counterbid']
    bid_register_df.loc[pj,'Profit'] = \
            va_coil_msg_df.loc[0,'Profit']
    bid_register_df.loc[pj,'bid_status'] = \
            va_coil_msg_df.loc[0,'bid_status']
    return(bid_register_df)

def compare_auctions(bid_register_df):
    """In the case coil agent receives more than 1 confirmation of auction won,
    this function compares the bid raised for each auction and reply accepting to the auction with the highest bid.
    This is also a way to assure that the coil agent is not accepting 2 processing at the same time"""
    df = bid_register_df[bid_register_df['won_bid'] == 1]
    df = df[df['accepted_bid'].isnull()]
    df = df.reset_index(drop=True)
    if df['second_bid'].isnull().sum() == 0:
        # if it has 1 confirmation coming from a initial bid and other from a second bid.
        # It compares bid and select the highest
        df = df.sort_values(by=['second_bid'], ascending=False)
        ca_agent_full_name = df.loc[0, 'auction_owner']
    else:
        idx = df.index[df['second_bid'].isnull()]
        c = []
        for i in idx:
            initial_bid = df.loc[i, 'initial_bid']
            c.append(initial_bid)
        df = df.sort_values(by=['second_bid'], ascending=False)
        max_second_bid = df.loc[0, 'second_bid']
        if not c:
            max_initial_bid = 0
        else:
            max_initial_bid = max(c)
        max_bid = max(max_initial_bid, max_second_bid)
        if max_second_bid == max_bid:
            idx = df.index[df['second_bid'] == max_bid].tolist()
        elif max_initial_bid == max_bid:
            idx = df.index[df['initial_bid'] == max_bid].tolist()
        ca_agent_full_name = df.loc[idx[0], 'auction_owner']
    return ca_agent_full_name

def send_activation_finish(my_full_name, ip_machine, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform status'
    df.loc[0, 'msg'] = 'change status'
    if level == 'start':
        df.loc[0, 'status'] = 'started'
    elif level == 'end':
        df.loc[0, 'status'] = 'ended'
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def find_br(my_full_name, msg,purpose):
    gbrw_jid = globals.gbrw_jid
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = purpose
    df.loc[0, 'msg'] = msg
    df.loc[0, 'to'] = gbrw_jid
    return df

def log_status(my_full_name, status, ip_machine):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    df.loc[0, 'status'] = status
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def aa_type(id):
    t = id.split('@')
    type = t[0]
    if type[0:2] == "lo":
        s = "log"
    elif type[0:2] == "nw":
        s = "nww"
    elif type[0:2] == "br":
        s = "browser"
    elif type[0:2] == "ca":
        s = "ca"
    elif type[0:2] == "wh":
        s = "wh"
    elif type[0:2] == "tc":
        s = "tc"
    elif type[0:2] == "la":
        s = "launcher"
    elif type[0:2] == "va":
        s = "va"
    else:
        s = "coil"
    return s

def rq_list(my_full_name, msg, agent, typed, sqn=0):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = typed
    df.loc[0,'seq'] = int(sqn)
    smsg = '[]'
    if type(msg) == list:
        smsg = json.dumps(msg)
    elif type(msg) == str:
        smsg = msg
    elif isinstance(msg,pd.DataFrame):
        smsg = msg.to_json(orient="records")

    df.loc[0, 'msg'] = smsg
    df.loc[0, 'to'] = agent
    return df

def contact_list_json(rq_contact_list, agent,typeans=0):
    agnt_jid = agent
    if '@' not in agent:
        if 'log' in agent:
            agnt_jid = globals.glog_jid
        elif 'bro' in agent:
            agnt_jid = globals.gbrw_jid
        elif 'lau' in agent:
            agnt_jid = globals.glhr_jid
        else:
            agents_df = globals.agnts_full
            agents_df = agents_df.loc[agents_df['Name'] == agent]
            agnt_jid = agents_df['User name'].iloc[-1]
    #
    rq_contact_list['type'] = typeans
    contact_list_msg = Message(to=agnt_jid)
    contact_list_msg.body = rq_contact_list.to_json(orient="records")
    contact_list_msg.set_metadata("performative", "inform")
    return contact_list_msg

def inform_coil_activation(my_full_name, code, agent_name, location):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'new_coil'
    df.loc[0, 'coil_code'] = code
    df.loc[0, 'agent_name'] = agent_name
    df.loc[0, 'coil_location'] = location
    df.loc[0, 'to'] = globals.glog_jid
    return df

def inform_new_order(my_full_name, msg):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'register_orders'
    df.loc[0, 'msg'] = msg
    return df

def inform_error(msg):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform error'
    df.loc[0, 'msg'] = msg
    return df.to_json(orient="records")

def inform_search(msg):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform search'
    df.loc[0, 'msg'] = msg
    return df.to_json(orient="records")

def inform_log(my_full_name, msg, agent):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform log'
    df.loc[0, 'from'] = my_full_name
    df.loc[0, 'to'] =  agent
    df.loc[0, 'msg'] = msg
    return df

#
# We assume coils agents will be named c*
#
def change_warehouse(launcher_df, clist, dact):
    #
    whc = launcher_df.loc[0, 'list_ware'].split(',')
    lc  = launcher_df.loc[0, 'list_coils'].split(',')
    ll  = launcher_df.loc[0, 'list_lengths'].split(',')
    wtt = launcher_df.loc[0, 'wait_time']
    bdg = launcher_df.loc[0, 'each_coil_price']
    odn = launcher_df.loc[0, 'order_code']
    pth = launcher_df.loc[0, 'string_operations']
    sgd = launcher_df.loc[0, 'steel_grade']
    thk = launcher_df.loc[0, 'thickness_coils']
    ach = launcher_df.loc[0, 'width_coils']
    sdt = launcher_df.loc[0, 'ship_date']
    parF = launcher_df.loc[0, 'param_f']
    pst = launcher_df.loc[0, 'prev_st']
    #
    j = 0
    my_dir = os.getcwd()
    # Look for parameters of the active coils represented by active agents.
    lik  = []
    #
    # Looking for c###@platform as representing coils ...
    if dact.shape[0] > 0:
        for k in dact['id'].to_list():
            ik = k[1:].split('@')[0]
            cnm= dact.loc[dact['id']==k,'code'].to_list()[0]
            if ik.isnumeric() and len(cnm) > 0:
                lik.append(int(ik))
    #
    lres = []
    for iz in range(len(lc)):
        # Potential coil names
        z = lc[iz] # name
        lz= ll[iz] # length
        pz= whc[iz]# pos
        if dact.shape[0] == 0:
            df = pd.DataFrame()
        else:
            df= dact[dact['code'].str.contains(z)]
        if df.shape[0] == 0:
            nid  = -1
            inid = True
            while inid:
               nid = nid + 1
               if nid not in lik:
                  lik.append(nid)
                  inid = False
            #
            sstr = str(int(wtt))
            sbdg = str(int(bdg))
            oshl = "/usr/bin/nohup /usr/bin/python3 "+my_dir+"/coil.py "
            oshl = oshl+" -w 12000 -v "+ sstr
            oshl = oshl+" -st 15000 -s on -l "+pz+" -b "+sbdg+" -c "+ z
            oshl = oshl+" -o " + odn + " -an " + str(nid) + " -ph " + pth
            oshl = oshl+" -ah " + str(int(ach)) + " -sg " + sgd
            oshl = oshl+" -thk " + str(float(thk)) + " -ll " + str(int(lz))
            oshl = oshl+" -sd " + sdt + " -F " + str(parF)
            oshl = oshl+" -pst " + pst
            oshl = oshl+" -bag "+globals.gbrw_jid+" -lag "+ globals.glog_jid
            oshl = oshl+" -u c"+"{0:0>3}".format(nid)+"@"
            oshl = oshl+globals.glog_jid.split('@')[1] + " -p "
            oshl = oshl+globals.glhr_pwd + " &"
            subprocess.Popen(oshl, stdout=None, stdin=None, stderr=None, \
                             close_fds=True, shell=True)
            id = "c"+"{0:0>3}".format(nid)+"@"+globals.glog_jid.split('@')[1]
            lres.append({'id':id,'code':z,'loc':pz,'bdg':bdg,'orden':odn,\
                         'ph':pth,'ancho':str(int(ach)),'sg':sgd,\
                         'espesor':str(float(thk)),'largo':str(int(lz)),\
                         'parF':str(parF),'prev_st':pst, 'sdate':sdt,'st':'ini'})
        else:
            id = df.iloc[0]['id']
            lres.append({'id':id,'code':z,'loc':pz,'bdg':bdg,'orden':odn,\
                         'ph':pth,'ancho':str(int(ach)),'sg':sgd,\
                         'espesor':str(float(thk)),'largo':str(int(lz)),\
                         'parF':str(parF),'prev_st':pst, 'sdate':sdt,'ph':pth,'st':'chg'})
        time.sleep(1)
    return(pd.DataFrame(lres))

def loc_of_coil(coil_df):
    loc_df = pd.DataFrame([], columns = ['location'])
    df = pd.read_csv('agents.csv', header=0, delimiter=",", engine='python')
    code = coil_df.loc[0, "Code"]
    location = df.loc[df.Code == code, 'location']
    location = location.values
    if location:
        location = location[0]
        loc_df.loc[0, 'location'] = location
        return loc_df
    else:
        coil_df = pd.DataFrame()
        return coil_df

def set_agent_parameters_coil(my_dir, my_name, my_full_name, \
                              ancho,esp,largo,sgrd,location, code,\
                              param_f,prev_station, path, sdate):
    df = pd.DataFrame([],columns=['id','mydir','agent','name','From',\
                                  'oname','budget','ancho','espesor',\
                                  'largo','sgrade','path','ship_date','int_fab','negotiation','bid','counterbid'])
    df.at[0,'id']    = my_full_name
    df.at[0,'mydir'] = my_dir
    df.at[0,'agent'] = my_full_name.split('@')[0]
    df.at[0,'name']  = my_name
    df.at[0,'coil_jid']  = my_full_name.split('@')[0]
    df.at[0,'agent_type']  = 'COIL'
    df.at[0,'From']   = location
    df.at[0,'prev_st'] = prev_station
    df.at[0,'oname'] = code
    df.at[0,'budget']= 0
    df.at[0,'ancho'] = ancho
    df.at[0,'espesor'] = esp
    df.at[0,'largo'] = largo
    df.at[0,'peso'] = float(largo) * float(ancho) * float(esp) * (1/1000) *(7.85)
    df.at[0,'sgrade']= sgrd
    df.at[0,'path']  = path
    df.at[0,'param_f']= param_f
    df.at[0,'ship_date'] = sdate
    df.at[0,'int_fab'] = 0
    df.at[0, 'negotiation'] = 0
    return(df)

def request_browser(df, seq, list):
    df.loc[:, 'id':'request_type']
    df.loc[0, 'to'] = 'browser@apiict00.etsii.upm.es'
    df.loc[0, 'msg'] = seq
    df.loc[0, 'coils'] = str(list)
    df = df[['id', 'purpose', 'request_type', 'msg', 'to']]
    return df

def answer_va(df_br, sender, df_va, coils, location):
    df = pd.DataFrame()
    df.loc[0, 'msg'] = df_va.loc[0, 'seq']
    df.loc[0, "id"] = 'browser'
    df.loc[0, "coils"] = coils
    df.loc[0, "location"] = location
    df.loc[0, "purpose"] = 'answer'
    df.loc[0, "to"] = sender
    df = df[['id', 'purpose', 'msg', 'coils', 'location', 'to']]
    return df

def bids_mean(medias_list):
    if len(medias_list) > 3:
        medias_list = medias_list[-3:]
    medias_list = statistics.mean(medias_list)
    return medias_list

def send_va(my_full_name, number, bid_mean, auction_level, jid_list):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'request_type'] = 'auction'
    if auction_level == 1:
        df.loc[0, 'msg'] = 'send pre-auction'
    elif auction_level == 2:
        df.loc[0, 'msg'] = 'send auction'
    elif auction_level == 3:
        df.loc[0, 'msg'] = 'send acceptance'
    df.loc[0, 'number'] = number
    df.loc[0, 'bid_mean'] = bid_mean
    df.loc[0, 'to'] = json.dumps(jid_list)
    df.loc[0, 'IP'] = globals.IP
    return df

def send_to_va_msg(my_full_name, bid, to, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'inform'
    if level == '1':
        df.loc[0, 'msg'] = 'send bid'
        df.loc[0, 'Bid'] = bid
    elif level == 2:
        df.loc[0, 'msg'] = 'send counterbid'
        df.loc[0, 'counterbid'] = bid
    df.loc[0, 'to'] = to
    return df

def va_msg_to(msg_body,agent):
    """Returns msg object without destination"""
    msg_tr = Message()
    msg_tr.body = msg_body
    msg_tr.to   = agent
    msg_tr.set_metadata("performative", "inform")
    return msg_tr

def auction_entry(va_data_df, coil_df,number):
    dif_ancho = int(coil_df.loc[0,'ancho'])- \
                int(va_data_df.loc[0,'coil_width'])
    dif_largo = int(coil_df.loc[0,'largo']) - \
                int(va_data_df.loc[0,'coil_length'])
    dif_espesor= float(coil_df.loc[0,'espesor']) - \
                 float(va_data_df.loc[0,'coil_thickness'])
    dif_total = float(dif_ancho + dif_espesor)
    if (dif_total <= 250) or (number >= 5):
        if (va_data_df.loc[0, 'id'] == 'va_08' or \
            va_data_df.loc[0, 'id'] == 'va_09') and (
                coil_df.loc[0, 'loc'] == 'K'):
            answer = 1
        elif (va_data_df.loc[0, 'id'] == 'va_10' or \
              va_data_df.loc[0, 'id'] == 'va_11') and \
             (coil_df.loc[0, 'loc'] == 'L'):
            answer = 1
        elif (va_data_df.loc[0, 'id'] == 'va_12') and (
                coil_df.loc[0, 'loc'] == 'M' or \
                coil_df.loc[0, 'loc'] == 'N'):
            answer = 1
        else:
            answer = 0
    else:
        answer = 0
    return answer

def create_bid(coil_df, bid_mean):
    #
    # persistence factor
    if coil_df.loc[0, 'number_auction'] <= 3:
        valor_1 = 0.15 * coil_df.loc[0, 'budget']
    elif coil_df.loc[0, 'number_auction'] > 3 and \
         coil_df.loc[0, 'number_auction'] <= 7:
        valor_1 = 0.23 * coil_df.loc[0, 'budget']
    else:
        valor_1 = 0.4 * coil_df.loc[0, 'budget']
    #
    # Date limit
    nhours = (datetime.datetime.strptime(coil_df.loc[0, \
                    'ship_date'],'%Y-%m-%d') - \
              datetime.datetime.now()).total_seconds()/3600
    if nhours <= 30:
        valor_2 = 0.25 * coil_df.loc[0, 'budget']
    else:
        valor_2 = 0.15 * coil_df.loc[0, 'budget']
    offer = 0.5 * bid_mean + valor_1 + valor_2
    return min(offer, float(coil_df.loc[0,'budget']))

def create_counterbid(msg_va, coil_df):
    if msg_va.loc[0,'position'] <= 2:
        valor_1 = 0.7 * coil_df.loc[0, 'budget_remaining']
    else:
        valor_1 = 0.8 * coil_df.loc[0, 'budget_remaining']
    contraoferta = valor_1 + coil_df.loc[0, 'bid']
    return contraoferta

def compare_va(va_coil_msg_df, bid_register_df):
    va_coil_msg_df['winning_auction'] = va_coil_msg_df['counterbid']
    results = pd.concat([bid_register_df, va_coil_msg_df])
    results = results.sort_values(by=['winning_auction'])
    results = results.reset_index(drop=True)
    coil_name_winner = results.loc[0, 'User_name_va']
    return coil_name_winner

def log_req_va (my_full_name,req, seq, agnt):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'request_type'] = req
    df.loc[0, 'msg'] = seq
    df.loc[0, 'to'] = agnt
    return df

def won_auction(my_full_name, va_coil_msg_sender_f, this_time):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = f'won auction in {va_coil_msg_sender_f}'
    df.loc[0, 'time'] = this_time
    return df.to_json(orient="records")

#NWW/COIL Functions

def log_req_nww (my_full_name,req, seq, agnt):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'request_type'] = req
    df.loc[0, 'msg'] = seq
    df.loc[0, 'to'] = agnt
    return df

def F_groups(parameter_F, nww_id):
    if (nww_id == 'nww_01' or nww_id == 'nww_04'):
        group_1=[1,21,25,27,50,52,53]
        group_2=[2,20,24,51,57]
        group_3=[3,22,58]
        group_4=[4,28]
    else:
        group_1=[1,10,11,20,24,40,43,44,45,46,47,48,60,61,63,64,70]
        group_2=[2,21,25,27,41,42]
        group_3=[3,22]
        group_4=[4,28]

    list = [group_1, group_2, group_3, group_4]

    for i in list:
        if parameter_F in i:
            parameter_F_group=i[0]
            break
        else:
            parameter_F_group = parameter_F

    return parameter_F_group

def nww_coil_enter_auction_rating(auction_agent_df, agent_df, not_entered_auctions):
    """Gives rating to auctionable resource. Returns 1 if positive, 0 if negative"""
    width_difference = int(auction_agent_df.loc[0,'ancho']) - int(agent_df.loc[0,'ancho'])
    value2=0
    value3=0
    value4=0
    #Conditions NWW1 & NWW4
    if (((auction_agent_df.loc[0, 'id'] == 'nww_01') and (agent_df.loc[0, 'From'] == 'F')) or ((auction_agent_df.loc[0, 'id'] == 'nww_04') and (agent_df.loc[0, 'From'] == 'I'))):
        if (auction_agent_df.loc[0,'lot_size']>300000) or (auction_agent_df.loc[0,'F_group'] == agent_df.loc[0,'F_group']):
            if (width_difference >= 0):
                    value1 = 100
            else:
                    value1 = 25   #hard condition (width jumps from wide to narrow)(No lo cumple:25)
        else:
             if (width_difference >= 0):
                     value1 = 50 #soft condition (500t within one surface group) (No lo cumple:50)
             else:
                     value1 = 12 #hard condition and soft condition are not matched (width jumps from wide to narrow)

        #Add a plus if coil has not entered 6 auctions
        if (not_entered_auctions>=6):
            plus=50
        else:
            plus=0
        entry=value1+plus

    #Conditions NWW3
    elif (auction_agent_df.loc[0, 'id'] == 'nww_03') and (agent_df.loc[0,'From'] == 'G'):
        if (auction_agent_df.loc[0,'param_f']>19) and (auction_agent_df.loc[0,'param_f']<29):
            if (agent_df.loc[0,'param_f']==10) or (agent_df.loc[0,'param_f']==11):
                if (width_difference >= 0):
                    value3=25 #Hard condition (after surface code 20 to 28 no surface code 10+11):
                else:
                    value3=6 #(width jumps and after surface code 20 to 28 no surface code 10+11 are not matched)(both hard conditions)
        if (agent_df.loc[0,'param_f']==40) or (agent_df.loc[0,'param_f']==41):
            if (auction_agent_df.loc[0,'lot_size']>400000):
                if (width_difference >= 0):
                    value2=100
                else:
                    value2=25
            else:
                if (width_difference >= 0):
                    if (auction_agent_df.loc[0,'param_f'] == agent_df.loc[0,'param_f']):
                        value2=100
                    else:
                        value2=50 #soft condition (min 400t)
                else:
                    if (auction_agent_df.loc[0,'param_f'] == agent_df.loc[0,'param_f']):
                        value2=25
                    else:
                        value2=12 #soft + hard condition (min 400t)
        elif (agent_df.loc[0,'param_f']==10) or (agent_df.loc[0,'param_f']==11):
            if (auction_agent_df.loc[0,'param_f'] == agent_df.loc[0,'param_f']):
                if (auction_agent_df.loc[0,'lot_size'] + agent_df.loc[0,'peso'])>300000:
                    if (width_difference >= 0):
                        value2=50 #soft condition (max 300t)
                    else:
                        value2=12
                else:
                    if (width_difference >= 0):
                        value2=100
                    else:
                        value2=25
            else:
                value2=100

        else:
            value4=100 #no more surface conditions (only for 40/41 10/11)

        value = value2 + value3 + value4
        #Add a plus if coil has not entered 6 auctions
        if (not_entered_auctions>=6):
            plus=50
        else:
            plus=0

        entry=value+plus
    else:
        entry=0

    return entry

def nww_coil_bid (nww_agent_df, coil_df, agent_status_var, coil_enter_auction_rating):
    """Creates bid or counterbid"""
    budget = coil_df.loc[0, 'budget']
    print(f'budget:{budget}')
    auction_level_bid = 0
    int_fab_bid = 0
    ship_date_bid = 0
    coil_rating = 0
    a = nww_agent_df.loc[0, 'auction_level']
    if agent_status_var == "auction":
        if nww_agent_df.loc[0, 'bid_status'] == 'bid':
            auction_level_bid = 0.10 * budget  # extra 10% if it is in first state of auction
        elif nww_agent_df.loc[0, 'bid_status'] == 'extrabid':
            auction_level_bid = 0.20 * budget  # extra 20% if it is in extrabid state of auction
        else:
            auction_level_bid = 0

        if coil_df.loc[0, 'int_fab'] == 1:
            int_fab_bid = 0.15 * budget
        else:
            int_fab_bid = 0

    nhours = (datetime.datetime.strptime(coil_df.loc[0, \
                    'ship_date'],'%Y-%m-%d') - \
              datetime.datetime.now()).total_seconds()/3600
    if nhours <= 30:
        ship_date_bid = 0.20 * coil_df.loc[0, 'budget']
    else:
        ship_date_bid = 0.10 * coil_df.loc[0, 'budget']

    coil_rating=coil_enter_auction_rating*budget/388.89 #it can represent up to 45% of the budget (max coil_enter_auction_rating is 175) (175*100/x) (sustituir x por porcentaje deseado y cambiar ese numero por el 388)


    co_bid = float(auction_level_bid) + float(int_fab_bid) + float(ship_date_bid) + float(coil_rating)

    return co_bid

def send_to_nww_msg(my_full_name, bid, to, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'inform'
    if level == '1':
        df.loc[0, 'msg'] = 'send bid'
        df.loc[0, 'Bid'] = bid
    elif level == 2:
        df.loc[0, 'msg'] = 'send counterbid'
        df.loc[0, 'counterbid'] = bid
    df.loc[0, 'to'] = to
    return df

def nww_create_counterbid(msg_nww, coil_df, id):
    position = msg_nww.loc[msg_nww['id'] == id, 'position'].values
    position=position[0]
    if position <= 2:
        valor_1 = 0.7 * coil_df.loc[0, 'budget_remaining']
    else:
        valor_1 = 0.7001 * coil_df.loc[0, 'budget_remaining']
    contraoferta = valor_1 + coil_df.loc[0, 'bid']
    return contraoferta

def update_bid_nww(bid_register_df,nww_coil_msg_df):
    pj = bid_register_df.index[bid_register_df['id']==\
        nww_coil_msg_df.loc[0,'Coil']]
    bid_register_df.loc[pj,'Bid'] = nww_coil_msg_df.loc[0,'Bid']
    bid_register_df.loc[pj,'Minimum_price'] = nww_coil_msg_df.loc[\
            0,'Minimum_price']
    bid_register_df.loc[pj,'Difference'] = nww_coil_msg_df.loc[\
            0,'Difference']
    bid_register_df.loc[pj,'Budget_remaining'] = \
            nww_coil_msg_df.loc[0,'Budget_remaining']
    bid_register_df.loc[pj,'Counterbid'] = \
            nww_coil_msg_df.loc[0,'Counterbid']
    bid_register_df.loc[pj,'Profit'] = \
            nww_coil_msg_df.loc[0,'Profit']
    bid_register_df.loc[pj,'bid_status'] = \
            nww_coil_msg_df.loc[0,'bid_status']
    return(bid_register_df)

def nww_to_coils_initial_df(agent_df, nww_prev_coil, lot_size):
    """Builds df to send to coils with auction information made by agent_df and last dimensional values and other parameters"""
    agent_df.at[0, 'largo'] = nww_prev_coil.loc[0, 'largo']
    agent_df.at[0, 'ancho'] = nww_prev_coil.loc[0, 'ancho']
    agent_df.at[0, 'espesor'] = nww_prev_coil.loc[0, 'espesor']
    agent_df.at[0, 'param_f'] = nww_prev_coil.loc[0, 'param_f']
    agent_df.at[0, 'F_group'] = nww_prev_coil.loc[0,'F_group']
    agent_df.at[0, 'lot_size'] = lot_size
    return agent_df

def send_nww(my_full_name, number, auction_level, jid_list):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'request_type'] = 'auction'
    if auction_level == 1:
        df.loc[0, 'msg'] = 'send initial bid'
    elif auction_level == 2:
        df.loc[0, 'msg'] = 'send counterbid'
    elif auction_level == 3:
        df.loc[0, 'msg'] = 'send acceptance'
    df.loc[0, 'number'] = number
    df.loc[0, 'to'] = json.dumps(jid_list)
    df.loc[0, 'IP'] = globals.IP
    return df

def nww_msg_to(msg_body,agent):
    """Returns msg object without destination"""
    msg_tr = Message()
    msg_tr.body = msg_body
    msg_tr.to   = agent
    msg_tr.set_metadata("performative", "inform")
    return msg_tr

def nww_bid_evaluation(coil_msgs_df, nww_data_df, step):
    key = []
    transport_cost_df = coste_transporte(nww_data_df.loc[0,'id'].split('@')[0])
    for i in range(transport_cost_df.shape[0]):
        m = transport_cost_df.loc[i, 'CrossTransport']
        n = transport_cost_df.loc[i, 'Supply']
        key.append(n+m)
    transport_cost_df['transport_cost'] = key
    transport_cost_df = transport_cost_df.loc[:, ['prev_st', 'To', 'transport_cost']]
    for i in range(coil_msgs_df.shape[0]):
        coil_msgs_df.at[i, 'production_cost'] = 2 * coil_msgs_df.at[i,'largo'] * coil_msgs_df.at[i,'ancho']* 10**-6
    #
    coil_msgs_df = coil_msgs_df.merge(transport_cost_df, on='prev_st', sort=False)
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'production_cost']
        n = coil_msgs_df.loc[i, 'transport_cost']
        coil_msgs_df.loc[i, 'minimum_price'] = m + n
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'minimum_price']
        if step == 'counterbid':
            n = coil_msgs_df.loc[i, 'counterbid']
            coil_msgs_df.loc[i, 'profit'] = n - m
        n = coil_msgs_df.loc[i, 'bid']
        coil_msgs_df.loc[i, 'difference'] = n - m
    if step == 'bid':
        results = coil_msgs_df[['agent_type', 'id', 'coil_jid', 'bid', \
                            'minimum_price','difference', 'ancho', 'largo',\
                            'espesor', 'ship_date','budget_remaining','negotiation','F_group','peso']]
        results = results.sort_values(by=['difference'], ascending = False, ignore_index=True)
        for i in range(len(results['id'].tolist())):
            results.at[i, 'rating_dif'] = results.loc[0, 'difference'] - results.loc[i, 'difference']
            if results.loc[i, 'rating_dif'] <= 1:
                results.at[i, 'negotiation'] = 1
            else:
                results.at[i, 'negotiation'] = 0

    else: # 'counterbid'
        results = coil_msgs_df[['agent_type', 'id', 'coil_jid', 'bid', \
                            'minimum_price', 'ancho', 'largo','difference',\
                            'espesor', 'ship_date','budget_remaining',\
                            'counterbid','profit','User_name_nww','F_group','peso']]
        results = results.sort_values(by=['profit'], ascending = False)
    #
    results = results.reset_index(drop=True)
    value = []
    for i in range(results.shape[0]):
        value.append(i+1)
    results.insert(loc=0, column='position', value=value)
    return results

def coste_transporte(to):
    costes_df = pd.DataFrame()
    costes_df['prev_st']= ['BA_01', 'BA_01', 'BA_01', 'BA_02', 'BA_02', 'BA_02', 'CA_03', 'CA_03', 'CA_03', 'CA_04', 'CA_04', 'CA_04', 'CA_05', 'CA_05', 'CA_05']
    costes_df['CrossTransport'] = [0,89.3, 85.6, 89.3, 0, 0, 87, 31.2, 27.6, 85.6, 58.8, 0, 85.6, 58.8, 0]
    costes_df['Supply'] = [17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5]
    costes_df['To'] = ['nww_01', 'nww_03', 'nww_04', 'nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04']
    costes_df = costes_df.loc[costes_df['To'] == to]
    costes_df = costes_df.reset_index(drop=True)
    return costes_df

def nww_negotiate(evaluation_df, coil_msgs_df):
    """Returns a df with coils to send message asking to counterbid"""
    negotiate_list = []
    for i in range(len(evaluation_df['id'].tolist())):
        if evaluation_df.loc[i, 'negotiation'] == 1:
            negotiate_list.append(evaluation_df.loc[i, 'id'])
    df = pd.DataFrame()
    for i in negotiate_list:
        df0 = evaluation_df.loc[evaluation_df['id'] == i]
        df = df.append(df0)
        df = df.reset_index(drop=True)
    return df

def nww_result(coil_ofertas_df, jid_list,step):
    df = pd.DataFrame([], columns=['position','Coil', 'Minimum_price', 'Bid',\
                                   'Difference', 'Budget_remaining', 'F_group'])
    if step == 'counterbid' :
        df = pd.DataFrame([], columns=['Coil', 'Minimum_price', 'Bid',\
                                   'Difference', 'Budget_remaining',\
                                   'Counterbid','Profit'])
    #
    for i in range(len(jid_list)):
        df.at[i, 'position'] = coil_ofertas_df.loc[i, 'position']
        df.at[i, 'Coil'] = coil_ofertas_df.loc[i, 'id']
        df.at[i, 'Minimum_price'] = coil_ofertas_df.loc[i, 'minimum_price']
        df.at[i, 'Bid'] = coil_ofertas_df.loc[i, 'bid']
        df.at[i, 'Difference'] = coil_ofertas_df.loc[i, 'difference']
        df.at[i, 'Budget_remaining'] = coil_ofertas_df.loc[i, 'budget_remaining']
        df.at[i, 'F_group'] = coil_ofertas_df.loc[i,'F_group']
        df.at[i, 'peso'] = coil_ofertas_df.loc[i,'peso']

        if step == 'counterbid' :
            df.at[i, 'Counterbid'] = coil_ofertas_df.loc[i, 'counterbid']
            df.at[i, 'Profit'] = coil_ofertas_df.loc[i, 'profit']
    return df

def auction_nww_kpis(agent_df, coil_msgs_df, auction_df, process_df,\
                 nww_counter_bid_df, *args):
    """Creates a df with all auction information"""
    df = auction_blank_df()
    df1 = pd.DataFrame()
    if args:
        winner = df1.loc[0, 'id']
    else:
        winner = coil_msgs_df.loc[0, 'id']
    df.at[0, 'id'] = agent_df.loc[0, 'id']
    df.at[0, 'agent_type'] = agent_df.loc[0, 'agent_type'].upper()
    df.at[0, 'location_1'] = coil_msgs_df.loc[0, 'From']
    df.at[0, 'location_2'] = 'END'
    df.at[0, 'location'] = (agent_df.loc[0, 'id'].split('@')[0]).upper()
    # winner coil info
    df.at[0, 'coil_auction_winner'] = coil_msgs_df.loc[0,'id']
    df.at[0, 'auction_number'] = auction_df.loc[0, 'number_auction_completed']
    df.at[0, 'coil_location_1'] = coil_msgs_df.loc[0, 'From']
    df.at[0, 'largo'] = coil_msgs_df.loc[0, 'largo']
    df.at[0, 'ancho'] = coil_msgs_df.loc[0, 'ancho']
    df.at[0, 'espesor'] = coil_msgs_df.loc[0, 'espesor']
    df.at[0, 'peso'] = coil_msgs_df.loc[0, 'peso']
    df.at[0, 'param_f'] = coil_msgs_df.loc[0, 'param_f']
    df.at[0, 'int_fab'] = auction_df.loc[0, 'int_fab']
    df.at[0, 'bid'] = nww_counter_bid_df.loc[0, 'Bid']
    df.at[0, 'budget'] = coil_msgs_df.loc[0, 'budget']
    df.at[0, 'ship_date'] = coil_msgs_df.loc[0, 'ship_date']
    df.at[0, 'setup_speed'] = agent_df.loc[0, 'setup_speed']
    # df.at[0, 'T1'] = coil_msgs_df.loc[0, 'T1']
    # df.at[0, 'T2'] = coil_msgs_df.loc[0, 'T2']
    # df.at[0, 'T3'] = coil_msgs_df.loc[0, 'T3']
    # df.at[0, 'T4'] = coil_msgs_df.loc[0, 'T4']
    # df.at[0, 'T5'] = coil_msgs_df.loc[0, 'T5']
    # df.at[0, 'q'] = coil_msgs_df.loc[0, 'q']
    # df.at[0, 'T1dif'] = coil_msgs_df.loc[0, 'T1dif']
    # df.at[0, 'T2dif'] = coil_msgs_df.loc[0, 'T2dif']
    # df.at[0, 'T3dif'] = coil_msgs_df.loc[0, 'T3dif']
    # df.at[0, 'T4dif'] = coil_msgs_df.loc[0, 'T4dif']
    # df.at[0, 'T5dif'] = coil_msgs_df.loc[0, 'T5dif']
    # df.at[0, 'total_temp_dif'] = coil_msgs_df.loc[0, 'total_temp_dif']
    # df.at[0, 'temp_rating'] = coil_msgs_df.loc[0, 'temp_rating']
    df.at[0, 'bid_rating'] = nww_counter_bid_df.loc[0, 'Counterbid'] / \
                nww_counter_bid_df.loc[0, 'Bid']
    df.at[0, 'Profit']  = nww_counter_bid_df.loc[0, 'Profit']
    df.at[0, 'ship_date_rating'] = auction_df.loc[0, 'ship_date_rating']
    df.at[0, 'int_fab_priority'] = auction_df.loc[0, 'int_fab_priority']
    df.at[0, 'int_fab_rating'] = auction_df.loc[0, 'int_fab_rating']
    df.at[0, 'rating'] = auction_df.loc[0, 'rating']
    df.at[0, 'rating_dif'] = auction_df.loc[0, 'rating_dif']
    df.at[0, 'negotiation'] = auction_df.loc[0, 'negotiation']
    # auction info
    df.at[0, 'pre_auction_start'] = auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_start'] = auction_df.loc[0, 'auction_start']
    df.at[0, 'auction_finish'] = datetime.datetime.now()
    df.at[0, 'active_tr_slot_1'] = auction_df.loc[0, 'active_tr_slot_1']
    df.at[0, 'active_tr_slot_2'] = auction_df.loc[0, 'active_tr_slot_2']
    df.at[0, 'tr_booking_confirmation_at'] = auction_df.loc[0, 'tr_booking_confirmation_at']
    df.at[0, 'active_wh'] = auction_df.loc[0, 'active_wh']
    df.at[0, 'wh_booking_confirmation_at'] = auction_df.loc[0, 'wh_booking_confirmation_at']
    df.at[0, 'wh_location'] = auction_df.loc[0, 'wh_location']
    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'auction_coils'] = auction_df.loc[0, 'auction_coils']
    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'brAVG(tr_op_time)'] = auction_df.loc[0, 'brAVG(tr_op_time)']
    df.at[0, 'brAVG(ca_op_time)'] = auction_df.loc[0, 'brAVG(ca_op_time)']
    op_times_df = op_times(process_df, agent_df)
    df.at[0, 'fab_start'] = process_df['fab_start'].iloc[-1]
    df.at[0, 'fab_end'] = process_df['fab_end'].iloc[-1]
    df.at[0, 'AVG(tr_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[\
                                    0, 'AVG(tr_op_time)'])
    df.at[0, 'AVG(ca_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[\
                                    0, 'AVG(ca_op_time)'])
    # df.at[0, 'slot_1_start'] = auction_df.loc[0, 'slot_1_start']
    # df.at[0, 'slot_1_end'] = auction_df.loc[0, 'slot_1_end']
    # df.at[0, 'slot_2_start'] = auction_df.loc[0, 'slot_2_start']
    # df.at[0, 'slot_2_end'] = auction_df.loc[0, 'slot_2_end']
    # df.at[0, 'name_tr_slot_1'] = auction_df.loc[0, 'name_tr_slot_1']
    # df.at[0, 'name_tr_slot_2'] = auction_df.loc[0, 'name_tr_slot_2']
    # df.at[0, 'delivered_to_wh'] = auction_df.loc[0, 'delivered_to_wh']
    # df.at[0, 'handling_cost_slot_1'] = auction_df.loc[0, 'handling_cost_slot_1']
    # df.at[0, 'handling_cost_slot_2'] = auction_df.loc[0, 'handling_cost_slot_2']
    # df.at[0, 'coil_ratings_1'] = auction_df.loc[0, 'coil_ratings_1']
    # df.at[0, 'coil_ratings_2'] = auction_df.loc[0, 'coil_ratings_2']
    # df.at[0, 'pre_auction_duration'] = auction_df.loc[0, 'wh_booking_confirmation_at'] - \
    #                     auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_duration'] = df.loc[0, 'auction_finish'] - \
                        auction_df.loc[0, 'auction_start']
    gantt_df = gantt(df)
    df.at[0, 'gantt'] = gantt_df.to_dict()
    location_diagram_df = location_diagram(df)
    df.at[0, 'location_diagram'] = location_diagram_df.to_dict()
    return df
