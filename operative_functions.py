import pandas as pd
import os
import datetime
from datetime import timedelta
from datetime import date
from random import randrange
from random import random
from random import randint
from spade.message import Message
import math
import json
import numpy as np
import subprocess
import time


"""General Functions"""

def agents_data():
    """This is a file from which all functions read information. It contains crucial information of the system:
    -jids, passwords, agent_full_names, pre-stablished location and WH capacity"""
    agents_data_df = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
    return agents_data_df

def agent_jid(agent_directory, agent_full_name):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    jid_direction = agents_df.loc[agents_df.Name == agent_full_name, 'User name']
    jid_direction = jid_direction.values
    jid_direction = jid_direction[0]
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
    elif agent_name == "launcher":
        full_name = agent_name
    else:
        if len(str(agent_number)) == 1:
            decimal = str(0)
        elif len(str(agent_number)) == 2:
            decimal = ""
        full_name = str(agent_name) + str("_") + decimal + str(agent_number)
    return full_name


def activation_df(agent_full_name, status_started_at, *args):
    agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    act_df = agent_data_df.loc[:, 'id':'activation_time']
    act_df = act_df.astype(str)
    act_df.at[0, 'purpose'] = "inform"
    act_df.at[0, 'request_type'] = ""
    act_df.at[0, 'time'] = datetime.datetime.now()
    act_df.at[0, 'status'] = "on"
    act_df.at[0, 'activation time'] = status_started_at
    if args:
        df = args[0]
        act_df = act_df.join(df)
    act_json = act_df.to_json()
    return act_json


def inform_log_df(agent_full_name, status_started_at, status, *args, **kwargs):
    """Inform of agent status"""
    agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    inf_df = agent_data_df.loc[:, 'id':'activation_time']
    inf_df = inf_df.astype(str)
    inf_df.at[0, 'purpose'] = "inform"
    inf_df.at[0, 'request_type'] = ""
    inf_df.at[0, 'time'] = datetime.datetime.now()
    inf_df.at[0, 'status'] = status
    inf_df.at[0, 'activation time'] = status_started_at
    if args:
        inf_df.at[0, 'to_do'] = args[0]  # In the case of stand-by coil, it passes to_do = "searching_auction" so that browser can search this coil when a resource looks for processing.
    if kwargs:  # in case did not enter auction
        inf_df.at[0, 'entered_auction'] = kwargs[0]  # "No, temp difference out of limit"
    return inf_df


def msg_to_log(msg_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "log"]
    log_jid = agents_df['User name'].iloc[-1]
    msg_log = Message(to=log_jid)
    print(f'msg_body:{msg_body}')
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
    df = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location',
                                   'coil_auction_winner', 'coil_length', 'coil_width', 'coil_thickness', 'coil_weight',
                                   'int_fab', 'bid', 'budget', 'ship_date', 'ship_date_rating',
                                   'setup_speed', 'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'T1dif', 'T2dif', 'T3dif', 'T4dif', 'T5dif', 'total_temp_dif', 'temp_rating',
                                   'bid_rating', 'int_fab_priority', 'int_fab_rating', 'rating', 'rating_dif', 'negotiation',
                                   'pre_auction_start', 'auction_start', 'auction_finish',
                                   'active_tr_slot_1', 'active_tr_slot_2', 'tr_booking_confirmation_at', 'active_wh', 'wh_booking_confirmation_at', 'wh_location', 'active_coils', 'auction_coils',
                                   'brAVG(tr_op_time)', 'brAVG(nww_op_time)','brAVG(nww_op_time)', 'AVG(tr_op_time)', 'AVG(nww_op_time)','AVG(nww_op_time)', 'fab_start'
                                   'slot_1_start', 'slot_1_end', 'slot_2_start', 'slot_2_end', 'name_tr_slot_1', 'name_tr_slot_2', 'delivered_to_wh', 'handling_cost_slot_1', 'handling_cost_slot_2',
                                   'coil_ratings_1', 'coil_ratings_2',
                                   'pre_auction_duration', 'auction_duration',
                                   'gantt', 'location_diagram'
                                   ])
    return df


def set_agent_parameters(agent_directory, agent_name, agent_full_name):
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time', 'int_fab'])
    agent_data.at[0, 'id'] = agent_full_name
    agent_data.at[0, 'agent_type'] = agent_name
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    if agent_name == "wh":
        agent_data.at[0, 'location'] = agents_df.at[0, 'Location']
        agent_data.at[0, 'capacity'] = agents_df.at[0, 'Capacity']
        agent_data.at[0, 'load'] = 0
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time', 'coil_in', 'coil_out', 'rack', 'capacity', 'load'])
    elif agent_name == "coil":
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time', 'to_do', 'entered_auction', 'int_fab', 'bid', 'bid_status', 'coil_length', 'coil_width', 'coil_thickness', 'paramater_F', 'coil_weight', 'setup_speed', 'budget', 'ship_date'])
        agent_data = coil_parameters(agent_data, agents_df, agent_full_name)
    elif agent_name == "tc":
        agent_data.at[0, 'location'] = agents_df.at[0, 'Location']
    elif agent_name == "nww":
        agent_data=agent_data.reindex(columns=['id','agent_type','location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time','coil_length', 'coil_width', 'coil_thickness', 'coil_weight','lot_size'])
        agent_data=nww_parameters(agent_data,agents_df,agent_full_name)
    else:
        agents_df = agents_data()
        df = agents_df.loc[agents_df['Name'] == agent_full_name]
        df = df.reset_index(drop=True)
        agent_data.at[0, 'location'] = df.at[0, 'Location']

    return agent_data


"""Agent-specific Functions"""

# Not used anymore. Register is in log_agent
# def wh_create_register(agent_directory, agent_full_name):
#     # wh registers entrance and exit of coils as well as reservations.
#     agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
#     wh_register_df = agent_data_df.drop(agent_data_df.columns.difference(['select columns to drop']), 1, inplace=True)
#     wh_register_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}_register.csv', index=False, header=True)


def coil_parameters(agent_data, agents_df, agent_full_name):
    """Sets pseudo random parameters"""
    rn = random()
    agent_data.at[0, 'int_fab'] = 0
    agent_data.at[0, 'location'] = agents_df.loc[0, 'Location']
    agent_data.at[0, 'to_do'] = 0
    agent_data.at[0, 'From'] = agents_df.loc[0, 'From']
    agent_data.at[0, 'Code'] = agents_df.loc[0, 'Code']
    agent_data.at[0, 'coil_length'] = 2000000 + (rn*2000000)  # between 2000 - 4000 m (2000000-4000000 mm)
    agent_data.at[0, 'coil_width'] = 900 + (rn*200)  # between 900 - 1100 mm
    agent_data.at[0, 'coil_thickness'] = 0.4 + (rn*0.5)  # between 0.4 - 0.9 mm
    agent_data.at[0, 'coil_weight'] = agent_data.at[0, 'coil_length'] * agent_data.at[0, 'coil_width'] * agent_data.at[0, 'coil_thickness'] * (1/1000) * (1/1000) *(7.85) #7.85 g/cm^3 is the density. Kg
    agent_data.at[0,'parameter_F']= randint(10,79)
    agent_data.at[0,'F_group'] = ''
    agent_data.at[0, 'setup_speed'] = 10 + (rn/2)  # between 10-10.5 m/s. Fab takes between 8 and 10 min with this conditions. process time = length / speed
    agent_data.at[0, 'ship_date'] = random_date(datetime.datetime.now(), datetime.datetime.now() + datetime.timedelta(minutes=40))  # Planning: between now and in 40 min.
    if rn < 0.15:
        agent_data.at[0, 'budget'] = 20000 + (20 * random())

    else:
        agent_data.at[0, 'budget'] = 20000
    # if rn < 0.2:
    #     agent_data.at[0, 'location'] = "I"
    # elif 0.2 < rn < 0.4:
    #     agent_data.at[0, 'location'] = "J"
    # elif 0.4 < rn < 0.6:
    #     agent_data.at[0, 'location'] = "K"
    # elif 0.6 < rn < 0.8:
    #     agent_data.at[0, 'location'] = "L"
    # elif 0.8 < rn < 1:
    #     agent_data.at[0, 'location'] = "M"
    return agent_data

def F_groups(parameter_F, nww_id):
    if (nww_id == 'nww_01' or nww_id == 'nww_04'):
        group_1=['group_1',21,25,27,50,52,53]
        group_2=['group_2',20,24,51,57]
        group_3=['group_3',22,58]
        group_4=['group_4',28]
    else:
        group_1=['group_1',10,11,20,24,40,43,44,45,46,47,48,60,61,63,64,70]
        group_2=['group_2',21,25,27,41,42]
        group_3=['group_3',22]
        group_4=['group_4',28]

    list = [group_1, group_2, group_3, group_4]

    for i in list:
        if parameter_F in i:
            parameter_F_group=i[0]
            break
        else:
            parameter_F_group = parameter_F

    return parameter_F_group



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
    agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    if agent_data_df.loc[0, 'capacity'] == agent_data_df.loc[0, 'load']:
        msg_body = "negative"
    elif agent_data_df.loc[0, 'capacity'] > agent_data_df.loc[0, 'load']:
        msg_body = "positive"
    else:
        msg_body = "negative"
    return msg_body


def wh_append_booking(agent_full_name, agent_directory, nww_df):
    """Adds +1 to load of WH and registers reservation"""
    # count load on agent_data_df
    agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    agent_data_df.loc[0, 'load'] = int(agent_data_df.loc[0, 'load']) + 1
    agent_data_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}.csv', index=False, header=True)
    # create msg body to send to log with booking info
    data_to_save = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'purpose', 'request_type', 'time', 'rack', 'coil_in', 'coil_out', 'capacity', 'load'])
    data_to_save.at[0, 'id'] = nww_df.loc[0, 'id']
    data_to_save.at[0, 'agent_type'] = nww_df.loc[0, 'agent_type']
    data_to_save.at[0, 'location_1'] = nww_df.loc[0, 'location_1']
    data_to_save.at[0, 'location_2'] = nww_df.loc[0, 'location_2']
    data_to_save.at[0, 'location'] = nww_df.loc[0, 'location']
    data_to_save.at[0, 'purpose'] = nww_df.loc[0, 'purpose']
    data_to_save.at[0, 'request_type'] = nww_df.loc[0, 'action']  # action=book
    data_to_save.at[0, 'time'] = datetime.datetime.now()
    data_to_save.at[0, 'rack'] = 1
    # print(f'{agent_book} booked rack on {agent_full_name}. Added +1 load to {agent_full_name} capacity')
    return data_to_save.to_json()


def wh_register(agent_full_name, agent_df):
    data_to_save = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'purpose', 'request_type', 'time', 'rack', 'coil_in', 'coil_out', 'capacity', 'load'])
    data_to_save.at[0, 'id'] = agent_df.loc[0, 'id']
    data_to_save.at[0, 'time'] = datetime.datetime.now()
    data_to_save.at[0, 'rack'] = 1
    data_to_save.at[0, 'request_type'] = agent_df.loc[0, 'action']
    if agent_df.loc[0, 'action'] == "out":
        # append coil entrance to wh_register
        data_to_save.at[0, 'coil_out'] = agent_df[0, 'coil_out']  # from received nww_df
        # discount load on agent_data_df
        agent_data_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
        agent_data_df.loc[0, 'load'] = int(agent_data_df.loc[0, 'Load']) - 1
        data_to_save.at[0, 'load'] = agent_data_df.loc[0, 'load']
    elif agent_df.loc[0, 'action'] == "in":
        # append coil entrance to wh_register
        data_to_save.at[0, 'coil_in'] = agent_df[0, 'coil_in']  # from received nww_df
        # We don´t count +1 when coil enters on wh, we already counted +1 at the booking.
    return data_to_save.to_json()


def tr_create_booking_register(agent_directory, agent_full_name):
    tr_register_df = pd.DataFrame([], columns=['day_minute', 'booking_type', 'assigned_to', 'assigned_at'])
    for i in range(1440):
        tr_register_df.at[i, 'day_minute'] = i + 1
    tr_register_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}_booking.csv', index=False, header=True)


def slot_to_minutes(agent_df):
    """It generates a list with the time passed to minute number of the day. Each day has 1440 minutes. min 1 = 00.00h, minute 1440 = 23.59h"""
    slot_range = []
    if agent_df.loc[0, 'slot'] == 1:
        # transform data to datetime type
        agent_df['slot_1_start'] = pd.to_datetime(agent_df['slot_1_start'], unit='ms')
        agent_df['slot_1_end'] = pd.to_datetime(agent_df['slot_1_end'], unit='ms')
        slot_1_start = agent_df['slot_1_start']
        slot_1_end = agent_df['slot_1_end']
        # transform datetime to minute of the day
        slot_1_start_min = math.floor((int(agent_df.loc[0, 'slot_1_start'].strftime("%H")) * 60) + (int(agent_df.loc[0, 'slot_1_start'].strftime("%M"))) + (int(agent_df.loc[0, 'slot_1_start'].strftime("%S")) / 60))
        slot_1_end_min = math.ceil((int(agent_df.loc[0, 'slot_1_end'].strftime("%H")) * 60) + (int(agent_df.loc[0, 'slot_1_end'].strftime("%M"))) + (int(agent_df.loc[0, 'slot_1_end'].strftime("%S")) / 60))
        slot_range = list(range(slot_1_start_min, slot_1_end_min+1))
    elif agent_df.loc[0, 'slot'] == 2:
        # transform data to datetime type
        agent_df['slot_2_start'] = pd.to_datetime(agent_df['slot_2_start'], unit='ms')
        agent_df['slot_2_end'] = pd.to_datetime(agent_df['slot_2_end'], unit='ms')
        # transform datetime to minute of the day
        slot_2_start_min = math.floor((int(agent_df.loc[0, 'slot_2_start'].strftime("%H")) * 60) + (int(agent_df.loc[0, 'slot_2_start'].strftime("%M"))) + (int(agent_df.loc[0, 'slot_2_start'].strftime("%S")) / 60))
        slot_2_end_min = math.ceil((int(agent_df.loc[0, 'slot_2_end'].strftime("%H")) * 60) + (int(agent_df.loc[0, 'slot_2_end'].strftime("%M"))) + (int(agent_df.loc[0, 'slot_2_end'].strftime("%S")) / 60))
        # create a list with the slot_ranges that need to be pre-booked
        slot_range = list(range(slot_2_start_min, slot_2_end_min))
    return slot_range


def tr_check_availability(agent_directory, agent_full_name, slot_range):
    """Checks availability of tr agent and returns a positive or negative msg"""
    tr_create_booking_register(agent_directory, agent_full_name)  # CHANGE THIS WHEN POSSIBLE. IT IS ERRASING ALL BOOKINGS. NOW THE SYSTEM IS NOT CONSTRAINT IN TR RESOURCES.
    tr_booking_df = pd.read_csv(f'{agent_directory}''/'f'{agent_full_name}_booking.csv', header=0, delimiter=",", engine='python')
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
        tr_msg_nww_body = "negative"
    else:
        tr_msg_nww_body = "positive"
    return tr_msg_nww_body


def tr_append_booking(agent_directory, agent_full_name, agent_df, slot_range):
    """Appends pre-booking or booking to booking register and returns booking info as a json"""
    tr_booking_df = pd.read_csv(f'{agent_directory}''/'f'{agent_full_name}_booking.csv', header=0, delimiter=",", engine='python')
    tr_booking_df['booking_type'] = tr_booking_df['booking_type'].fillna("")
    for y in slot_range:
        tr_booking_df.loc[y - 1, 'assigned_to'] = agent_df.loc[0, 'id']
        tr_booking_df.loc[y - 1, 'assigned_at'] = datetime.datetime.now()
        if agent_df.loc[0, 'action'] == "booked":
            tr_booking_df.loc[y - 1, 'booking_type'] = "booked"
        elif agent_df.loc[0, 'action'] == "pre-book":
            tr_booking_df.loc[y - 1, 'booking_type'] = "pre-book"
        tr_booking_df.to_csv(f'{agent_directory}''/'f'{agent_full_name}_booking.csv', index=False, header=True)
    return tr_booking_df.to_json()


def req_active_users_loc_times(agent_df, *args):
    """Returns msg body to send to browser as a json"""
    agent_request_df = agent_df.loc[:, 'id':'time']
    agent_request_df = agent_request_df.astype(str)
    agent_request_df.at[0, 'purpose'] = "request"
    this_time = datetime.datetime.now()
    agent_request_df.at[0, 'time'] = this_time
    if args:
        agent_request_df.at[0, 'request_type'] = args[0]
    else:
        agent_request_df.at[0, 'request_type'] = "active users location & op_time"
    return agent_request_df.to_json()


def msg_to_br(msg_body, agent_directory):
    """Returns msg object to send to browser agent"""
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    jid = agents_df['User name'].iloc[-1]
    msg_br = Message(to=jid)
    msg_br.body = msg_body
    msg_br.set_metadata("performative", "inform")
    return msg_br


def br_jid(agent_directory):
    """Returns str with browser jid"""
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    jid = agents_df['User name'].iloc[-1]
    return jid


def estimate_tr_slot(br_data_df, fab_started_at, leeway, agent_df):
    """Returns a df with the the calculated time slots for which tr is requested"""
    a = br_data_df.loc[0, 'AVG(nww_op_time)']
    b = br_data_df.loc[0, 'AVG(tr_op_time)']
    nww_estimated_end = fab_started_at + datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(nww_op_time)']))  # time when on going fab started + mean nww processing time.
    if br_data_df.loc[0, 'AVG(nww_op_time)'] == 9:
        if br_data_df.loc[0, 'AVG(tr_op_time)'] == 3.5:
            slot_1_start = nww_estimated_end - datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(tr_op_time)'])) - (leeway / 2)
            slot_1_end = nww_estimated_end + (leeway / 2)

    slot_1_start = nww_estimated_end - datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(tr_op_time)'])) - (leeway / 2)
    slot_1_end = nww_estimated_end + (leeway / 2)
    slot_2_start = nww_estimated_end + datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(nww_op_time)'])) - datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(tr_op_time)']/2)) - (leeway / 2)
    slot_2_end = nww_estimated_end + datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(nww_op_time)'])) + datetime.timedelta(minutes=int(br_data_df.loc[0, 'AVG(tr_op_time)']/2)) + (leeway / 2)  # time when on going fab started + mean mww processing time + mean tr operation time
    nww_to_tr_df = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'action', 'time', 'slot_1_start', 'slot_1_end', 'slot_2_start', 'slot_2_end', 'slot'])
    nww_to_tr_df.at[0, 'id'] = agent_df.loc[0, 'id']
    nww_to_tr_df.at[0, 'agent_type'] = agent_df.loc[0, 'agent_type']
    nww_to_tr_df.at[0, 'location_1'] = agent_df.loc[0, 'location_1']
    nww_to_tr_df.at[0, 'location_2'] = agent_df.loc[0, 'location_2']
    nww_to_tr_df.at[0, 'location'] = agent_df.loc[0, 'location']
    nww_to_tr_df.at[0, 'purpose'] = "request"
    nww_to_tr_df.at[0, 'slot_1_start'] = slot_1_start
    nww_to_tr_df.at[0, 'slot_1_end'] = slot_1_end
    nww_to_tr_df.at[0, 'slot_2_start'] = slot_2_start
    nww_to_tr_df.at[0, 'slot_2_end'] = slot_2_end
    this_time = datetime.datetime.now()
    nww_to_tr_df.at[0, 'time'] = this_time
    nww_to_tr_df.at[0, 'request_type'] = "request"
    nww_to_tr_df.at[0, 'action'] = "pre-book"
    return nww_to_tr_df



def handling_cost(agent_to_tr_df, slot):
    slot_total_minutes = ""
    if slot == 1:
        slot_total_minutes = agent_to_tr_df.at[0, 'slot_1_end'] - agent_to_tr_df.at[0, 'slot_1_start']
    elif slot == 2:
        slot_total_minutes = agent_to_tr_df.at[0, 'slot_2_end'] - agent_to_tr_df.at[0, 'slot_2_start']
    print(f'slot_total_minutes: {slot_total_minutes}')

    print(f'slot_total_sec: {slot_total_minutes.total_seconds()}')
    handling_cost = slot_total_minutes.total_seconds() * (50 / 3600)  # 50€/h
    print(f'handling_cost: {handling_cost}')
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
    agent_locations_dist_df = pd.DataFrame(data=d)
    return agent_locations_dist_df

def get_tr_list(slot, br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name) of active tr agents"""
    agent_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    agents_df = agents_data()
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no sé si debería cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "tc"]
    br_data_df = br_data_df.reset_index(drop=True)
    to = str()
    if slot == 1:
        nww_location_1 = agent_df.loc[0, 'location_1']
        br_data_df['location_nww'] = str(nww_location_1)  ### location 1!!!!
        br_data_df['dash'] = "-"
        br_data_df["from_to"] = br_data_df["location_nww"] + br_data_df["dash"] + br_data_df["location"]
        to = "location_" + nww_location_1  # location 1!!!!!
    elif slot == 2:
        nww_location_2 = agent_df.loc[0, 'location_2']
        br_data_df['location_nww'] = str(nww_location_2)  ### location 2!!!!
        br_data_df['dash'] = "-"
        br_data_df["from_to"] = br_data_df["location_nww"] + br_data_df["dash"] + br_data_df["location"]
        to = "location_" + nww_location_2  # location 2!!!!!
    active_users_location_df = br_data_df
    agent_locations_dist_df = locations_min_distances()
    agent_locations_dist_df = agent_locations_dist_df[['id_min', to]]
    tr_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in tr_list:
        a = agent_locations_dist_df.loc[agent_locations_dist_df[to] == i]
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
    segment_df = segment_df.reset_index(drop=True)  # segment_df contains the location of active tr and id_name sorted by shortest distance to them
    tr_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in tr_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(columns={'from_to': 'segment'})
    #print(f'active_users_location_df: {active_users_location_df}')
    #print(f'segment_df: {segment_df}')
    results = active_users_location_df.merge(segment_df, on='segment')
    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def get_wh_list(br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name) of active wh agents"""
    agent_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    agents_df = agents_data()
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no sé si debería cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "wh"]
    br_data_df = br_data_df.reset_index(drop=True)
    to = str()
    nww_location_2 = agent_df.loc[0, 'location_2']
    br_data_df['location_nww'] = str(nww_location_2)  ### location 2!!!!
    br_data_df['dash'] = "-"
    br_data_df["from_to"] = br_data_df["location_nww"] + br_data_df["dash"] + br_data_df["location"]
    to = "location_" + nww_location_2  # location 2!!!!!
    active_users_location_df = br_data_df
    agent_locations_dist_df = locations_min_distances()
    agent_locations_dist_df = agent_locations_dist_df[['id_min', to]]
    wh_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in wh_list:
        a = agent_locations_dist_df.loc[agent_locations_dist_df[to] == i]
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
    segment_df = segment_df.reset_index(drop=True)  # segment_df contains the location of active tr and id_name sorted by shortest distance to them
    tr_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in tr_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(columns={'from_to': 'segment'})
    print(f'active_users_location_df: {active_users_location_df}')
    print(f'segment_df: {segment_df}')
    results = active_users_location_df.merge(segment_df, on='segment')
    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def get_coil_list(br_data_df, agent_full_name, agent_directory):
    """Returns a df containing name, location and jid_name (User_name) of active coil agents. Coils are sorted by distance"""
    agent_df = pd.read_csv(f'{agent_full_name}.csv', header=0, delimiter=",", engine='python')
    agents_df = agents_data()
    br_data_df['new_col'] = br_data_df['agent_type'].astype(str) ### esto no sé si debería cambiarlo
    br_data_df = br_data_df.loc[br_data_df['new_col'] == "coil"]
    to = str()
    nww_location_1 = agent_df.loc[0, 'location_1']
    br_data_df['location_nww'] = str(nww_location_1)  ### location 1!!!!
    br_data_df['dash'] = "-"
    br_data_df["from_to"] = br_data_df["location_nww"] + br_data_df["dash"] + br_data_df["location"]
    to = "location_" + nww_location_1  # location 1!!!!!
    active_users_location_df = br_data_df
    agent_locations_dist_df = locations_min_distances()
    agent_locations_dist_df = agent_locations_dist_df[['id_min', to]]
    coil_list = br_data_df['from_to'].tolist()
    values = []
    keys = []
    for i in coil_list:
        a = agent_locations_dist_df.loc[agent_locations_dist_df[to] == i]
        id_loop = a.loc[a.index[-1], 'id_min']
        coil_to_loop = a.loc[a.index[-1], to]
        keys.append(id_loop)
        values.append(coil_to_loop)
    segment = dict(zip(keys, values))
    segment_df = pd.DataFrame([segment])
    segment_df = segment_df.T
    indexes = segment_df.index.values.tolist()
    segment_df = segment_df.rename(columns={0: "segment"})
    segment_df.insert(loc=0, column='id_min', value=indexes)
    segment_df = segment_df.sort_values(by=['id_min'])
    segment_df = segment_df.reset_index(drop=True)  # segment_df contains the location of active coils and id_name sorted by shortest distance to them
    coil_list = active_users_location_df['agent'].tolist()
    jid_names = pd.DataFrame()
    for i in coil_list:
        a = agents_df.loc[agents_df['Name'] == i]
        jid_names = jid_names.append(a)
    active_users_location_df = active_users_location_df.rename(columns={'from_to': 'segment'})
    print(f'active_users_location_df: {active_users_location_df}')
    print(f'active_users_location_df: {segment_df}')
    results = active_users_location_df.merge(segment_df, on='segment')
    results = results.rename(columns={'agent': 'Name'})
    results = results.merge(jid_names, on='Name')
    results = results.sort_values(by=['id_min'])
    results = results[['Name', 'location', 'segment', 'id_min', 'User name']]
    return results


def nww_msg_to(msg_body):
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
    if args == "coils":  # if nww is requesting
        df = df.loc[0, 'to_do'] == "search_auction"  # filters coils searching for auction
    return df


def check_active_users_loc_times(agent_name, *args):
    """Returns a json with tr & nww averages operation time"""
    if args == "coils":
        df = br_get_requested_df(agent_name, args)
    else:
        df = br_get_requested_df(agent_name)
    # Calculate means
    df['time'] = pd.to_datetime(df['time'])
    df['AVG(tr_op_time)'] = pd.to_datetime(df['AVG(tr_op_time)'], unit='ms')
    df['AVG(nww_op_time)'] = pd.to_datetime(df['AVG(nww_op_time)'], unit='ms')
    tr_avg = df['AVG(tr_op_time)'].mean()  # avg(operation_time_tr)
    nww_avg = df['AVG(nww_op_time)'].mean()  # avg(operation_time_nww)
    if pd.isnull(tr_avg):
        if pd.isnull(nww_avg):
            tr_avg = 3.5
            nww_avg = 9
    else:
        tr_avg = tr_avg - datetime.datetime(1970, 1, 1)
        nww_avg = nww_avg - datetime.datetime(1970, 1, 1)
        tr_avg = tr_avg.total_seconds() / 60
        nww_avg = nww_avg.total_seconds() / 60
    op_times_df = pd.DataFrame([], columns=['AVG(tr_op_time)', 'AVG(nww_op_time)'])
    op_times_df.at[0, 'AVG(tr_op_time)'] = tr_avg
    op_times_df.at[0, 'AVG(nww_op_time)'] = nww_avg
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


def confirm_tr_bookings_to_log(agent_to_tr_df, agent_directory, closest_tr_df, tr_assigned):
    """Builds the json to send to log after confirmed booking"""
    agents_df = agents_data()
    agent_to_log_df = agent_to_tr_df
    agent_to_log_df = agent_to_log_df.loc[:, 'id':'slot_2_end']
    agent_to_log_df["active_tr"] = ""
    agent_to_log_df.at[0, 'active_tr'] = closest_tr_df["Name"].tolist()
    agent_to_log_df.at[0, 'purpose'] = "inform"
    agent_to_log_df.at[0, 'request_type'] = ""
    agent_to_log_df.at[0, 'tr_slot_1'] = tr_assigned[0]
    agent_to_log_df.at[0, 'tr_slot_2'] = tr_assigned[1]
    this_time = datetime.datetime.now()
    agent_to_log_df.at[0, 'time'] = this_time
    agents_df = agents_df.rename(columns={'User name': 'tr_slot_1'})
    agents_df.drop(['Location', 'Capacity'], axis=1, inplace=True)
    agent_to_log_df = agent_to_log_df.merge(agents_df, on='tr_slot_1')
    agent_to_log_df = agent_to_log_df.rename(columns={'Name': 'name_tr_slot_1'})
    agents_df = agents_df.rename(columns={'tr_slot_1': 'tr_slot_2'})
    agent_to_log_df = agent_to_log_df.merge(agents_df, on='tr_slot_2')
    agent_to_log_df = agent_to_log_df.rename(columns={'Name': 'name_tr_slot_2'})
    agent_to_log_df.drop(['action', 'Location1_y', 'Location2_y', 'Location1_x', 'Location2_x'], axis=1, inplace=True)
    agent_to_log_df = agent_to_log_df.rename(columns={'tr_slot_1': 'jid_tr_slot_1'})
    agent_to_log_df = agent_to_log_df.rename(columns={'tr_slot_2': 'jid_tr_slot_2'})
    return agent_to_log_df.to_json()


def confirm_wh_booking_to_log(agent_to_wh_df, wh_assigned, agent_directory, closest_wh_df):
    agents_df = agents_data()
    this_time = datetime.datetime.now()
    agent_to_log_df = agent_to_wh_df
    agent_to_log_df.at[0, 'purpose'] = "inform"
    agent_to_log_df.at[0, 'request_type'] = ""
    agent_to_log_df.at[0, 'time'] = this_time
    wh_assigned_str = ""
    wh_assigned_str = wh_assigned_str.join(map(str, wh_assigned))
    agent_to_log_df['jid_wh'] = ""
    agent_to_log_df['name_wh'] = ""
    agent_to_log_df['active_wh'] = ""
    agent_to_log_df.at[0, 'active_wh'] = closest_wh_df["Name"].tolist()
    agent_to_log_df.at[0, 'jid_wh'] = wh_assigned_str
    agents_df.drop(['Location', 'Capacity'], axis=1, inplace=True)
    agents_df = agents_df.loc[agents_df['User name'] == wh_assigned_str]
    agents_df = agents_df.reset_index(drop=True)
    agent_to_log_df['name_wh'] = ""
    agent_to_log_df.at[0, 'name_wh'] = agents_df.loc[0, 'Name']
    return agent_to_log_df.to_json()


def plc_temp(coil_df):
    """Once auction is completed, temperatures of the oven are saved for next processing. It returns the information as df
    """


def nww_auction_df():
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time', 'T1', 'T2', 'T3', 'T4', 'T5', 'q'])


def nww_assigned_auction_df():
    agent_data = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location', 'purpose', 'request_type', 'time', 'activation_time', 'T1', 'T2', 'T3', 'T4', 'T5', 'q', 'bid_status', 'bid', ])


def nww_to_coils_second_df(agent_df):
    """Builds df to send to coils with counterbid information made by agent_df"""


def coil_enter_auction_rating(auction_agent_df, agent_df, not_entered_auctions):
    """Gives rating to auctionable resource. Returns 1 if positive, 0 if negative"""
    width_difference=auction_agent_df.loc[0,'coil_width']-agent_df.loc[0,'coil_width']

    #Conditions NWW1 & NWW4
    if (((auction_agent_df.loc[0, 'id'] == 'nww_1') and (agent_df.loc[0, 'location'] == 'F')) or ((auction_agent_df.loc[0, 'id'] == 'nww_4') and (agent_df.loc[0, 'location'] == 'I'))):
        if ('CA' in agent_df[0,'From']):
            if (auction_agent_df.loc[0,'lot_size']>500000) or (auction_agent_df.loc[0,'F_group'] == agent_df.loc[0,'F_group']):
                if (width_difference >= 0):
                        value1 = 100
                else:
                        value1 = 25   #hard condition (width jumps from wide to narrow)(No lo cumple:25)

            else:
                 if (width_difference >= 0):
                         value1 = 50 #soft condition (500t within one surface group) (No lo cumple:50)

                 else:
                         value1 = 12 #hard condition and soft condition are not matched (width jumps from wide to narrow)

        else:
            if (auction_agent_df.loc[0,'lot_size']>300000) or (auction_agent_df.loc[0,'F_group'] == agent_df.loc[0,'F_group']):
                if (width_difference >= 0):
                        value1 = 100

                else:
                        value1 = 25 #hard condition (width jumps from wide to narrow)(No lo cumple:25)

            else:
                 if (width_difference >= 0):
                         value1 = 50 #soft condition (500t within one surface group) (No lo cumple:50)

                 else:
                         value1 = 12  #hard condition and soft conditions are not matched (width jumps from wide to narrow)

        #Add a plus if coil has not entered 6 auctions
        if (not_entered_auctions>=6):
            plus=50

        entry=value1+plus

    #Conditions NWW3
    elif (auction_agent_df.loc[0, 'id'] == 'nww_3') and (agent_df.loc[0,'location'] == 'G'):
        if (auction_agent_df.loc[0,'parameter_F']>19) and (auction_agent_df.loc[0,'parameter_F']<29):
            if (agent_df.loc[0,'parameter_F']==10) or (agent_df.loc[0,'parameter_F']==11):
                if (width_difference >= 0):
                    value3=25 #Hard condition (after surface code 20 to 28 no surface code 10+11):
                else:
                    value3=6 #(width jumps and after surface code 20 to 28 no surface code 10+11 are not matched)(both hard conditions)
        if (agent_df.loc[0,'parameter_F']==40) or (agent_df.loc[0,'parameter_F']==41):
            if (auction_agent_df.loc[0,'lot_size']>400000):
                if (width_difference >= 0):
                    value2=100
                else:
                    value2=25
            else:
                if (width_difference >= 0):
                    if (auction_agent_df.loc[0,'parameter_F'] == agent_df.loc[0,'parameter_F']):
                        value2=100
                    else:
                        value1=50 #soft condition (min 400t)
                else:
                    if (auction_agent_df.loc[0,'parameter_F'] == agent_df.loc[0,'parameter_F']):
                        value2=25
                    else:
                        value1=12 #soft + hard condition (min 400t)
        elif (agent_df.loc[0,'parameter_F']==10) or (agent_df.loc[0,'parameter_F']==11):
            if (auction_agent_df.loc[0,'parameter_F'] == agent_df.loc[0,'parameter_F']):
                if (auction_agent_df.loc[0,'lot_size'] + agent_df.loc[0,'coil_weight'])>300000:
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
            value1=100 #no more surface conditions (only for 40/41 10/11)

        value1=value2+value3
        #Add a plus if coil has not entered 6 auctions
        if (not_entered_auctions>=6):
            plus=50

        entry=value1+plus
    else:
        entry=0

    return entry




def coil_bid(nww_agent_df, coil_df, agent_status_var, coil_enter_auction_rating):
    """Creates bid or counterbid"""
    budget = agent_df.loc[0, 'budget']
    print(f'budget:{budget}')
    auction_level_bid = ""
    int_fab_bid = ""
    ship_date_bid=""
    coil_rating=""
    a = nww_agent_df.loc[0, 'auction_level']
    print(f'auction_level:{a}')
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

    if coil_df.loc[0, 'ship_date'] <= 10:
        ship_date_bid = 0.20 * coil_df.loc[0, 'budget']
    elif coil_df.loc[0, 'ship_date'] <= 20:
        ship_date_bid = 0.15 * coil_df.loc[0, 'budget']
    elif coil_df.loc[0, 'ship_date'] <= 30:
        ship_date_bid = 0.10 * coil_df.loc[0, 'budget']
    else:
        ship_date_bid = 0.05 * coil_df.loc[0, 'budget']

    coil_rating=coil_enter_auction_rating*budget/388.89 #it can represent up to 45% of the budget (max coil_enter_auction_rating is 175) (175*100/x) (sustituir x por porcentaje deseado y cambiar ese numero por el 500)

    print(f'loc_bid: {loc_bid}')
    print(f'auction_level_bid: {auction_level_bid}')
    print(f'int_fab_bid: {int_fab_bid}')
    print(f'ship_date_bid: {ship_date_bid}')
    co_bid = int(auction_level_bid) + int(int_fab_bid) + int(ship_date_bid) + int(coil_rating)

    return co_bid



def auction_kpis(agent_df, coil_msgs_df, auction_df, process_df, nww_counter_bid_df, *args):
    """Creates a df with all auction information"""
    df = auction_blank_df()
    df1 = pd.DataFrame()
    if args:
        df1 = args[0]
        print(f'df1: {df1}')
        print(f'args: {args}')
        winner = df1.loc[0, 'id']
    else:
        winner = coil_msgs_df.loc[0, 'id']
    nww_counter_bid_df = nww_counter_bid_df.loc[nww_counter_bid_df['id'] == winner]
    #nww agent info
    df.at[0, 'id'] = agent_df.loc[0, 'id']
    df.at[0, 'agent_type'] = agent_df.loc[0, 'agent_type']
    df.at[0, 'location_1'] = agent_df.loc[0, 'location_1']
    df.at[0, 'location_2'] = agent_df.loc[0, 'location_2']
    df.at[0, 'location'] = agent_df.loc[0, 'location']
    # winner coil info
    df.at[0, 'coil_auction_winner'] = winner
    df.at[0, 'coil_location_1'] = nww_counter_bid_df.loc[0, 'location']
    df.at[0, 'coil_length'] = nww_counter_bid_df.loc[0, 'coil_length']
    df.at[0, 'coil_width'] = nww_counter_bid_df.loc[0, 'coil_width']
    df.at[0, 'coil_thickness'] = nww_counter_bid_df.loc[0, 'coil_thickness']
    df.at[0, 'coil_weight'] = nww_counter_bid_df.loc[0, 'coil_weight']
    df.at[0, 'parameter_F'] = nww_counter_bid_df.loc[0, 'parameter_F']
    df.at[0, 'F_group'] = nww_counter_bid_df.loc[0, 'F_group']
    df.at[0, 'int_fab'] = nww_counter_bid_df.loc[0, 'int_fab']
    df.at[0, 'bid'] = nww_counter_bid_df.loc[0, 'bid']
    df.at[0, 'budget'] = nww_counter_bid_df.loc[0, 'budget']
    df.at[0, 'ship_date'] = nww_counter_bid_df.loc[0, 'ship_date']
    df.at[0, 'setup_speed'] = nww_counter_bid_df.loc[0, 'setup_speed']
    df.at[0, 'bid_rating'] = coil_msgs_df.loc[0, 'bid_rating']
    df.at[0, 'ship_date_rating'] = coil_msgs_df.loc[0, 'ship_date_rating']
    df.at[0, 'int_fab_priority'] = coil_msgs_df.loc[0, 'int_fab_priority']
    df.at[0, 'int_fab_rating'] = coil_msgs_df.loc[0, 'int_fab_rating']
    df.at[0, 'rating'] = coil_msgs_df.loc[0, 'rating']
    df.at[0, 'rating_dif'] = coil_msgs_df.loc[0, 'rating_dif']
    df.at[0, 'negotiation'] = coil_msgs_df.loc[0, 'negotiation']
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
    df.at[0, 'brAVG(nww_op_time)'] = auction_df.loc[0, 'brAVG(nww_op_time)']
    op_times_df = op_times(process_df, agent_df)
    df.at[0, 'fab_start'] = process_df['fab_start'].iloc[-1]
    df.at[0, 'fab_end'] = process_df['fab_end'].iloc[-1]
    df.at[0, 'AVG(tr_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[0, 'AVG(tr_op_time)'])
    df.at[0, 'AVG(nww_op_time)'] = datetime.timedelta(seconds=op_times_df.loc[0, 'AVG(nww_op_time)'])
    df.at[0, 'slot_1_start'] = auction_df.loc[0, 'slot_1_start']
    df.at[0, 'slot_1_end'] = auction_df.loc[0, 'slot_1_end']
    df.at[0, 'slot_2_start'] = auction_df.loc[0, 'slot_2_start']
    df.at[0, 'slot_2_end'] = auction_df.loc[0, 'slot_2_end']
    df.at[0, 'name_tr_slot_1'] = auction_df.loc[0, 'name_tr_slot_1']
    df.at[0, 'name_tr_slot_2'] = auction_df.loc[0, 'name_tr_slot_2']
    df.at[0, 'delivered_to_wh'] = auction_df.loc[0, 'delivered_to_wh']
    df.at[0, 'handling_cost_slot_1'] = auction_df.loc[0, 'handling_cost_slot_1']
    df.at[0, 'handling_cost_slot_2'] = auction_df.loc[0, 'handling_cost_slot_2']
    df.at[0, 'coil_ratings_1'] = auction_df.loc[0, 'coil_ratings_1']
    df.at[0, 'coil_ratings_2'] = auction_df.loc[0, 'coil_ratings_2']
    df.at[0, 'pre_auction_duration'] = auction_df.loc[0, 'wh_booking_confirmation_at'] - auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_duration'] = df.loc[0, 'auction_finish'] - auction_df.loc[0, 'auction_start']
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
    resource = [auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id']]
    coil = [auction_kpis_df.loc[0, 'coil_auction_winner'], auction_kpis_df.loc[0, 'coil_auction_winner'], auction_kpis_df.loc[0, 'coil_auction_winner'], auction_kpis_df.loc[0, 'coil_auction_winner']]
    loc_step = [1, 2, 3, 4]
    auction_id = ["", "", "", ""]
    location = [auction_kpis_df.loc[0, 'coil_location_1'], auction_kpis_df.loc[0, 'location_1'], auction_kpis_df.loc[0, 'location_2'], auction_kpis_df.loc[0, 'wh_location']]
    df = pd.DataFrame([], columns=['resource', 'coil', 'loc_step', 'auction_id', 'location'])
    df['resource'] = resource
    df['coil'] = coil
    df['loc_step'] = loc_step
    df['auction_id'] = auction_id
    df['location'] = location
    results = df.merge(coord_df, on='location')
    print(results)
    return results


def gantt(auction_kpis_df):
    df = pd.DataFrame([], columns=['task_id', 'task_name', 'duration', 'start', 'resource', 'complete'])
    task_id = [1, 2, 3, 4, 5]
    task_name = ['pre_auction', 'auction', 'tr_slot1', 'processing', 'tr_slot2']
    duration = [auction_kpis_df.loc[0, 'pre_auction_duration'], auction_kpis_df.loc[0, 'auction_duration'], auction_kpis_df.loc[0, 'slot_1_end'] - auction_kpis_df.loc[0, 'slot_1_start'], auction_kpis_df.loc[0, 'AVG(ca_op_time)'], auction_kpis_df.loc[0, 'slot_2_end'] - auction_kpis_df.loc[0, 'slot_2_start']]
    start = [auction_kpis_df.loc[0, 'pre_auction_start'], auction_kpis_df.loc[0, 'auction_start'], auction_kpis_df.loc[0, 'slot_1_start'], auction_kpis_df.loc[0, 'fab_start'], auction_kpis_df.loc[0, 'slot_2_start']]
    finish = [auction_kpis_df.loc[0, 'auction_start'], auction_kpis_df.loc[0, 'auction_finish'], auction_kpis_df.loc[0, 'slot_1_end'], auction_kpis_df.loc[0, 'fab_end'], auction_kpis_df.loc[0, 'slot_2_end']]
    resource = [auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'name_tr_slot_1'], auction_kpis_df.loc[0, 'id'], auction_kpis_df.loc[0, 'name_tr_slot_2']]
    complete = [100, 100, 100, 100, 100]
    df['task_id'] = task_id
    df['task_name'] = task_name
    df['duration'] = duration
    df['start'] = start
    df['finish'] = finish
    df['resource'] = resource
    df['complete'] = complete
    print(df)
    return df


def op_times(p_df, nww_data_df):
    df = nww_data_df
    df.at[0, 'AVG(nww_op_time)'] = p_df['processing_time'].iloc[-1]
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



def modify_nww_data_df(p_df, nww_data_df):
    """modifies agent_df with current parameters"""
    nww_data_df.at[0, 'coil_length'] = p_df['coil_length'].iloc[-1]
    nww_data_df.at[0, 'coil_width'] = p_df['coil_width'].iloc[-1]
    nww_data_df.at[0, 'coil_thickness'] = p_df['coil_thickness'].iloc[-1]
    nww_data_df.at[0, 'coil_weight'] = p_df['coil_weight'].iloc[-1]
    nww_data_df.at[0, 'parameter_F'] = p_df['parameter_F'].iloc[-1]
    nww_data_df.at[0, 'F_group'] = p_df['F_group'].iloc[-1]
    nww_data_df.loc[0, 'purpose'] = ''
    nww_data_df.loc[0, 'request_type'] = ''
    return nww_data_df


def get_agent_jid(agent_full_name, *args):
    agents_df = agents_data()
    df = agents_df.loc[agents_df['Name'] == agent_full_name]
    df = df.reset_index(drop=True)
    name = df.loc[0, 'User name']
    return name


def bid_register(agent_name, agent_full_name):
    """Creates bid register"""
    df = pd.DataFrame([], columns=['id', 'agent_type', 'auction_owner', 'initial_bid', 'second_bid', 'won_bid', 'accepted_bid'])
    #df.at[0, 'id'] = agent_full_name
    #df.at[0, 'agent_type'] = agent_name
    return df


def append_bid(bid, bid_register_df, agent_name, agent_full_name, nww_agent_df, bid_level, *args):
    """Appends bid and returns bid register"""
    """args: best_auction_agent_full_name"""
    df = pd.DataFrame([], columns=['id', 'agent_type', 'auction_owner', 'initial_bid', 'second_bid', 'won_bid', 'accepted_bid'])
    df.at[0, 'id'] = agent_full_name
    df.at[0, 'agent_type'] = agent_name
    nww_agent_full_name = nww_agent_df.loc[0, 'id']
    df.at[0, 'auction_owner'] = nww_agent_full_name
    if bid_level == 'initial':
        df.at[0, 'initial_bid'] = bid
        bid_register_df = bid_register_df.append(df)
        bid_register_df = bid_register_df.reset_index(drop=True)
    elif bid_level == 'extrabid':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == nww_agent_full_name]
        bid_register_df.at[idx, 'second_bid'] = bid
    elif bid_level == 'acceptedbid':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == nww_agent_full_name]
        bid_register_df.at[idx, 'won_bid'] = 1
    elif bid_level == 'confirm':
        idx = bid_register_df.index[bid_register_df['auction_owner'] == args]
        bid_register_df.at[idx, 'accepted_bid'] = 1
    return bid_register_df


def compare_auctions(bid_register_df):
    """In the case coil agent receives more than 1 confirmation of auction won,
    this function compares the bid raised for each auction and reply accepting to the auction with the highest bid.
    This is also a way to assure that the coil agent is not accepting 2 processing at the same time"""
    df = bid_register_df[bid_register_df['won_bid'] == 1]
    df = df[df['accepted_bid'].isnull()]
    df = df.reset_index(drop=True)
    if df['second_bid'].isnull().sum() == 0:  # if it has 1 confirmation coming from a initial bid and other from a second bid. It compares bid and select the highest
        df = df.sort_values(by=['second_bid'], ascending=False)
        nww_agent_full_name = df.loc[0, 'auction_owner']
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
        nww_agent_full_name = df.loc[idx[0], 'auction_owner']
    return nww_agent_full_name

#Temper rolling agent
def nww_parameters(agent_data, agents_df, agent_full_name):
    """Sets pseudo random parameters"""
    rn = random()
    agent_data.at[0, 'location_1'] = agents_df.at[0, 'Location1']
    agent_data.at[0, 'location_2'] = agents_df.at[0, 'Location2']
    agent_data.at[0, 'location'] = agents_df.at[0, 'Location']
    agent_data.at[0, 'coil_length'] = 2000000 + (rn*2000000)  # between 2000 - 4000 m (2000000-4000000 mm)
    agent_data.at[0, 'coil_width'] = 900+ (rn*200)  # between 900-1100 mm
    agent_data.at[0, 'coil_thickness'] = 0.4 + (rn*0.5)  # between 0.4-0.9 mm
    agent_data.at[0, 'coil_weight'] = agent_data.at[0, 'coil_length'] * agent_data.at[0, 'coil_width'] * agent_data.at[0, 'coil_thickness'] * (1/1000) * (1/1000) *(7.85) #7.85 g/cm^3 is the density. Kg
    agent_data.at[0,'parameter_F']= randint(10,79)
    agent_data.at[0,'F_group'] = F_groups(agent_data.at[0,'parameter_F'], agent_data.at[0,'id'])
    agent_data.at[0, 'setup_speed'] = 10 + (rn/2)  # between 10-10.5 m/s. Fab takes between 8 and 10 min with this conditions. process time = length / speed
    return agent_data

def nww_to_coils_initial_df(agent_df, nww_prev_coil, lot_size):
    """Builds df to send to coils with auction information made by agent_df and last dimensional values and other parameters"""
    agent_df.at[0, 'coil_width'] = nww_prev_coil.loc[0, 'coil_width']
    agent_df.at[0, 'coil_weight'] = nww_prev_coil.loc[0, 'coil_weight']
    agent_df.at[0, 'coil_thickness'] = nww_prev_coil.loc[0, 'coil_thickness']
    agent_df.at[0, 'parameter_F'] = nww_prev_coil.loc[0, 'parameter_F']
    agent_df.at[0, 'F_group'] = nww_prev_coil.loc[0,'F_group']
    agent_df.at[0, 'lot_size'] = lot_size
    return agent_df

def coste_transporte(to):
    costes_df = pd.DataFrame()
    costes_df['From']= ['BA_01', 'BA_01', 'BA_01', 'BA_02', 'BA_02', 'BA_02', 'CA_03', 'CA_03', 'CA_03', 'CA_04', 'CA_04', 'CA_04', 'CA_05', 'CA_05', 'CA_05']
    costes_df['CrossTransport'] = [0,89.3, 85.6, 89.3, 0, 0, 87, 31.2, 27.6, 85.6, 58.8, 0, 85.6, 58.8, 0]
    costes_df['Supply'] = [17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5, 17.5, 16.8, 6.5]
    costes_df['To'] = ['nww_01', 'nww_03', 'nww_04', 'nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04','nww_01', 'nww_03', 'nww_04']
    costes_df = costes_df.loc[costes_df['To'] == to]
    costes_df = costes_df.reset_index(drop=True)
    return costes_df

def nww_process_df(df, nww_counter_bid_df, nww_to_tr_df):
    """Adds new line to process_df with new parameters"""
    print(f'df0: {df}')
    process_df = df
    if pd.isnull(df['fab_start'].iloc[-1]):
        new_line_df = pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
                                index=['fab_start', 'processing_time', 'start_auction_before', 'start_next_auction_at', 'fab_end', 'origin','parameter_F','F_group', 'lot_sizes', 'coil_width','cooling_time'])
        process_df = df.append(new_line_df, ignore_index=True)
        process_df = process_df.reset_index(drop=True)
        print(f'process_df1: {process_df}')
        processing_time = (1/nww_counter_bid_df.loc[0, 'setup_speed']) * nww_counter_bid_df.loc[0, 'coil_length']
        process_df['start_auction_before'].iloc[-1] = 6.5 * 60
        start_auction_before = process_df['start_auction_before'].iloc[-1]
        process_df['processing_time'].iloc[-1] = processing_time
        process_df['fab_start'].iloc[-1] = (nww_to_tr_df.loc[0, 'slot_1_end'] - datetime.timedelta(minutes=2.5))
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + datetime.timedelta(seconds=processing_time)
        start_next_auction_at = process_df['fab_end'].iloc[-1] - datetime.timedelta(seconds=start_auction_before)
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
        a = process_df['fab_start'].iloc[-1]
        print(f'entered_option_1: {a}')
    else:
        process_df.loc[process_df.index.max() + 1, 'start_auction_before'] = ""
        processing_time = (1/nww_counter_bid_df.loc[0, 'setup_speed']) * nww_counter_bid_df.loc[0, 'coil_length']
        process_df['processing_time'].iloc[-1] = processing_time
        process_df['fab_start'].iloc[-1] = process_df['fab_end'].iloc[-2]
        a = process_df['fab_start']
        process_df['fab_start'] = pd.to_datetime(process_df['fab_start'])
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + datetime.timedelta(seconds=processing_time)
        process_df['start_auction_before'].iloc[-1] = process_df['start_auction_before'].iloc[-2]
        start_next_auction_at = process_df['fab_end'].iloc[-1] - datetime.timedelta(seconds=process_df['start_auction_before'].iloc[-1])
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
        a = process_df['fab_start'].iloc[-1]
        print(f'entered_option_2: {a}')
    process_df['coil_length'].iloc[-1] = nww_counter_bid_df.loc[0, 'coil_length']
    process_df['coil_thickness'].iloc[-1] = nww_counter_bid_df.loc[0, 'coil_thickness']
    process_df['coil_weight'].iloc[-1] = nww_counter_bid_df.loc[0, 'coil_weight']
    process_df['parameter_F'].iloc[-1] = nww_counter_bid_df.loc[0, 'parameter_F']
    process_df['F_group'].iloc[-1] = nww_counter_bid_df.loc[0, 'F_group']


    #process_df['fab_start'] = process_df['fab_start'].fillna("")
    print(f'process_df2: {process_df}')
    return process_df

def nww_auction_bid_evaluation(coil_msgs_df, agent_df):
    """Evaluates coils and their bids and returns a df with an extra column with rating to coils proposal"""
    ev_df = coil_msgs_df[['From','id', 'agent_type', 'location', 'int_fab', 'bid', 'bid_status', 'coil_length', 'coil_width', 'coil_thickness', 'coil_weight', 'setup_speed', 'origin','parameter_F','F_group','lot_sizes','cooling_time']]
    ev_df = ev_df.reset_index(drop=True)
    # Transport evaluation. Extra column with transport cost
    key = []
    transport_cost_df = coste_transporte(agent_df.loc[0,'id'])
    transport_cost_df['transport_cost']=transport_cost_df['CrossTransport']+transport_cost_df['Supply']
    transport_cost_df = transport_cost_df.loc[:, ['From', 'To', 'transport_cost']]

    # Production  evaluation. Extra column with production cost
    for i in range(len(coil_msgs_df['id'].tolist())):
        ev_df.at[i,'production_cost']=2*ev_df.at[i,'coil_length']*ev_df.at[i,'coil_widht']*10^(-6)
    ev_df = ev_df.reset_index(drop=True)

    #sum all and provide final cost.
    ev_df = ev_df.merge(transport_cost_df, on='From', sort=False)
    ev_df['total_cost']=ev_df['transport_cost']+ev_df['production_cost']
    ev_df['cost_bid_dif']=ev_df['bid']-ev_df['total_cost']
    ev_df=ev_df.loc[:,['agent_type','id','coil_jid','bid','total_cost','cost_bid_dif','coil_length', 'coil_width', 'coil_thickness','ship_date']]
    ev_df = ev_df.sort_values(by=['cost_bid_dif'], ascending=False) #de mayor a menor(mayor diferencia precio ofrecido menos coste, mejor situada la bobina)
    ev_df = ev_df.reset_index(drop=True)
    for i in range(len(ev_df['cost_bid_dif'].tolist())):
        ev_df.at[i, 'total_cost_bid_dif'] = ev_df.loc[0, 'cost_bid_dif'] - ev_df.loc[i, 'cost_bid_dif']
        if ev_df.loc[i, 'total_cost__bid_dif '] <= 2000:
            ev_df.at[i, 'negotiation'] = 1
        else:
            ev_df.at[i, 'negotiation'] = 0
    return ev_df


def nww_negotiate(evaluation_df, coil_msgs_df):
    """Returns a df with coils to send message asking to counterbid"""
    negotiate_list = []
    for i in range(len(evaluation_df['id'].tolist())):
        if evaluation_df.loc[i, 'negotiation'] == 1:
            negotiate_list.append(evaluation_df.loc[i, 'id'])
    df = pd.DataFrame()
    for i in negotiate_list:
        df0 = coil_msgs_df.loc[coil_msgs_df['id'] == i]
        df = df.append(df0)
        df = df.reset_index(drop=True)
    return df


"""Launcher functions"""

def send_activation_finish(my_full_name, ip_machine, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    if level == 'start':
        df.loc[0, 'status'] = 'started'
    elif level == 'end':
        df.loc[0, 'status'] = 'ended'
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def order_file(agent_full_name, order_code, steel_grade, thickness, width_coils, num_coils, list_coils, each_coil_price,
               list_ware, string_operations, wait_time):
    order_msg_log = pd.DataFrame([], columns=['id', 'order_code', 'steel_grade', 'thickness_coils', 'width_coils',
                                              'num_coils', 'list_coils', 'each_coil_price', 'string_operations',
                                              'date'])
    order_msg_log.at[0, 'id'] = agent_full_name
    order_msg_log.at[0, 'purpose'] = 'setup'
    order_msg_log.at[0, 'msg'] = 'new order'
    order_msg_log.at[0, 'order_code'] = order_code
    order_msg_log.at[0, 'steel_grade'] = steel_grade
    order_msg_log.at[0, 'thickness_coils'] = thickness
    order_msg_log.at[0, 'width_coils'] = width_coils
    order_msg_log.at[0, 'num_coils'] = num_coils
    order_msg_log.at[0, 'list_coils'] = list_coils
    order_msg_log.at[0, 'each_coil_price'] = each_coil_price
    order_msg_log.at[0, 'list_ware'] = list_ware
    order_msg_log.at[0, 'string_operations'] = string_operations
    order_msg_log.at[0, 'date'] = date.today().strftime('%Y-%m-%d')
    order_msg_log.at[0, 'to'] = 'log'
    order_msg_log.at[0, 'wait_time'] = wait_time
    return order_msg_log

def order_to_log(order_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "log"]
    log_jid = agents_df['User name'].iloc[-1]
    order_msg = Message(to=log_jid)
    order_msg.body = order_body
    order_msg.set_metadata("performative", "inform")
    return order_msg

def order_coil(la_json, code):
    df = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
    name = df.loc[df.Code == code, 'User name'].values
    name = name[0]
    msg_budget = Message()
    msg_budget.to = str(name)
    msg_budget.body = la_json
    return msg_budget

def order_budget(budget, code):
    df = pd.DataFrame()
    df_agents = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
    name = df_agents.loc[df_agents.Code == code, 'User name'].values
    name = name[0]
    df.loc[0, 'id'] = 'launcher'
    df.loc[0, 'purpose'] = 'setup'
    df.loc[0, 'msg'] = 'new budget'
    df.loc[0, 'budget'] = budget
    df.loc[0, 'code'] = str(code)
    df.loc[0, 'to'] = str(name)
    return df

def order_to_search(search_body, agent_full_name, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    browser_jid = agents_df['User name'].iloc[-1]
    search_msg = Message(to=browser_jid)
    search_msg.body = 'Search:' + search_body + ':' + agent_full_name
    search_msg.set_metadata("performative", "inform")
    return search_msg

def checkFileExistance():
    try:
        with open('ActiveAgents.csv', 'r') as f:
            return True
    except FileNotFoundError as e:
        return False
    except IOError as e:
        return False

def checkFile2Existance():
    try:
        with open('RegisterOrders.csv', 'r') as f:
            return True
    except FileNotFoundError as e:
        return False
    except IOError as e:
        return False

def update_coil_status(coil_id, status):
    df = pd.read_csv('RegisterOrders.csv', header=0, delimiter=",", engine='python')
    df.loc[(df.ID_coil.isin([coil_id])), 'coil_status'] = status
    df.to_csv('RegisterOrders.csv', index=False)

def change_warehouse(launcher_df, my_dir):
    nww = launcher_df.loc[0, 'list_ware'].split(',')
    lc = launcher_df.loc[0, 'list_coils'].split(',')
    wait_time = launcher_df.loc[0, 'wait_time']
    j = 0
    my_dir = os.getcwd()
    for z in lc:
        number = 1
        name = 'coil_00' + str(number)
        df = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
        for i in range(30):
            if df.loc[df.Name == name, 'Code'].isnull().any().any():
                cmd = f'python3 coil.py -an {str(number)} -l {nww[j]} -c{z} -w{wait_time}'
                subprocess.Popen(cmd, stdout=None, stdin=None, stderr=None, close_fds=True, shell=True)
                break
            elif df.loc[df.Name == name, 'Code'].values == z:
                cmd = f'python3 coil.py -an {str(number)} -l {nww[j]} -c{z} -w{wait_time}'
                subprocess.Popen(cmd, stdout=None, stdin=None, stderr=None, close_fds=True, shell=True)
                break
            else:
                number = number + 1
                if len(str(number)) < 2:
                    name = 'coil_00' + str(number)
                else:
                    name = 'coil_0' + str(number)
        time.sleep(5)
        j = j + 1

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

def log_status(my_full_name, status, ip_machine):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    df.loc[0, 'status'] = status
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def inform_error(msg):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform error'
    df.loc[0, 'msg'] = msg
    return df.to_json(orient="records")

def order_register(my_full_name, code, coils, locations):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'new order from launcher'
    df.loc[0, 'code'] = code
    df.loc[0, 'coils'] = coils
    df.loc[0, 'locations'] = locations
    return df.to_json(orient="records")



'''Functions to improve readability in messages. Improve functions'''
def answer_nww(df_br, sender, df_nww, coils, location):
    df = pd.DataFrame()
    df.loc[0, 'msg'] = df_nww.loc[0, 'seq']
    df.loc[0, "id"] = 'browser'
    df.loc[0, "coils"] = coils
    df.loc[0, "location"] = location
    df.loc[0, "purpose"] = 'answer'
    df.loc[0, "to"] = sender
    df = df[['id', 'purpose', 'msg', 'coils', 'location', 'to']]
    return df

def answer_coil(df, sender, seq_df):
    df.loc[0, 'msg'] = seq_df.loc[0, 'msg']
    df.loc[0, "id"] = 'browser'
    df.loc[0, "purpose"] = 'answer'
    df.loc[0, "to"] = sender
    df = df[['id', 'purpose', 'msg','location', 'to']]
    return df
