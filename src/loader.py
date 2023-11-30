import json
import argparse
import os
import io
import shutil
import copy
from datetime import datetime
from pick import pick
from time import sleep
#import globe
import pandas as pd

# combine all json file in all-weeks8-9

path_channel = "anonymaized/"
def slack_parser(path_channel):
    """ parse slack data to extract useful informations from the json file
        step of execution
        1. Import the required modules
        2. read all json file from the provided path
        3. combine all json files in the provided path
        4. extract all required informations from the slack data
        5. convert to dataframe and merge all
        6. reset the index and return dataframe
    """

    # specify path to get json files
    combined = []
    for json_file in glob.glob(f"{path_channel}*.json"):
        with open(json_file, 'r', encoding="utf8") as slack_data:
            combined.append(slack_data)

    # loop through all json files and extract required informations
    dflist = []
    for slack_data in combined:

        msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
        reply_count, reply_users_count, tm_thread_end = [],[],[],[],[],[],[],[],[],[]

        for row in slack_data:
            if 'bot_id' in row.keys():
                continue
            else:
                msg_type.append(row['type'])
                msg_content.append(row['text'])
                if 'user_profile' in row.keys(): sender_id.append(row['user_profile']['real_name'])
                else: sender_id.append('Not provided')
                time_msg.append(row['ts'])
                if 'blocks' in row.keys() and len(row['blocks'][0]['elements'][0]['elements']) != 0 :
                     msg_dist.append(row['blocks'][0]['elements'][0]['elements'][0]['type'])
                else: msg_dist.append('reshared')
                if 'thread_ts' in row.keys():
                    time_thread_st.append(row['thread_ts'])
                else:
                    time_thread_st.append(0)
                if 'reply_users' in row.keys(): reply_users.append(",".join(row['reply_users'])) 
                else:    reply_users.append(0)
                if 'reply_count' in row.keys():
                    reply_count.append(row['reply_count'])
                    reply_users_count.append(row['reply_users_count'])
                    tm_thread_end.append(row['latest_reply'])
                else:
                    reply_count.append(0)
                    reply_users_count.append(0)
                    tm_thread_end.append(0)
        data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st,
         reply_count, reply_users_count, reply_users, tm_thread_end)
        columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
         'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

        df = pd.DataFrame(data=data, columns=columns)
        df = df[df['sender_name'] != 'Not provided']
        dflist.append(df)

    dfall = pd.concat(dflist, ignore_index=True)
    dfall['channel'] = path_channel.split('/')[-1].split('.')[0]        
    dfall = dfall.reset_index(drop=True)
    
    return dfall

def parse_slack_reaction(path, channel):
    """get reactions"""
    dfall_reaction = pd.DataFrame()
    combined = []
    for json_file in glob.glob(f"{path}*.json"):
        with open(json_file, 'r') as slack_data:
            combined.append(slack_data)

    reaction_name, reaction_count, reaction_users, msg, user_id = [], [], [], [], []

    for k in combined:
        slack_data = json.load(open(k.name, 'r', encoding="utf-8"))
        
        for i_count, i in enumerate(slack_data):
            if 'reactions' in i.keys():
                for j in range(len(i['reactions'])):
                    msg.append(i['text'])
                    user_id.append(i['user'])
                    reaction_name.append(i['reactions'][j]['name'])
                    reaction_count.append(i['reactions'][j]['count'])
                    reaction_users.append(",".join(i['reactions'][j]['users']))
                
    data_reaction = zip(reaction_name, reaction_count, reaction_users, msg, user_id)
    columns_reaction = ['reaction_name', 'reaction_count', 'reaction_users_count', 'message', 'user_id']
    df_reaction = pd.DataFrame(data=data_reaction, columns=columns_reaction)
    df_reaction['channel'] = channel
    return df_reaction

def get_community_participation(path):
    """ specify path to get json files"""
    combined = []
    comm_dict = {}
    for json_file in glob.glob(f"{path}*.json"):
        with open(json_file, 'r') as slack_data:
            combined.append(slack_data)
    # print(f"Total json files is {len(combined)}")
    for i in combined:
        a = json.load(open(i.name, 'r', encoding='utf-8'))

        for msg in a:
            if 'replies' in msg.keys():
                for i in msg['replies']:
                    comm_dict[i['user']] = comm_dict.get(i['user'], 0)+1
    return comm_dict



# Create wrapper classes for using slack_sdk in place of slacker
class SlackDataLoader: 
    '''
    Slack exported data IO class.

    When you open slack exported ZIP file, each channel or direct message 
    will have its own folder. Each folder will contain messages from the 
    conversation, organised by date in separate JSON files.

    You'll see reference files for different kinds of conversations: 
    users.json files for all types of users that exist in the slack workspace
    channels.json files for public channels, 
    
    These files contain metadata about the conversations, including their names and IDs.

    For secruity reason, we have annonymized names - the names you will see are generated using faker library.
    
    '''
    def __init__(self, path):
        '''
        path: path to the slack exported data folder
        '''
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_ussers()
    

    def get_users(self):
        '''
        write a function to get all the users from the json file
        '''
        with open(os.path.join(self.path, 'users.json'), 'r') as f:
            users = json.load(f)

        return users
    
    def get_channels(self):
        '''
        write a function to get all the channels from the json file
        '''
        with open(os.path.join(self.path, 'channels.json'), 'r') as f:
            channels = json.load(f)

        return channels

    def get_channel_messages(self, channel_name):
        '''
        write a function to get all the messages from a channel
        
        '''
        channel_path = os.path.join(self.path, channel_name)
        message = []
        for json_file in glob.glob(f"{channel_path}/*.json"):
            with open(json_file, 'r', encodeing = 'utf8') as slack data:
                messages += json.load(slack_data)
                
                return messages

    # 
    def get_user_map(self):
        '''
        write a function to get a map between user id and user name
        '''
        userNamesById = {}
        userIdsByName = {}
        for user in self.users:
            userNamesById[user['id']] = user['name']
            userIdsByName[user['name']] = user['id']
        return userNamesById, userIdsByName      
    
def top_message_by_replies(df, column, n=10):
    top_reply_users = df.groupby('user')[column].sum().nlargest(n)
    bottom_reply_users = df.groupby('user')[column].sum().nsmallest(n)

    # Top and bottom 10 users by mention count
    top_mention_users = df.groupby('user')['mentions'].sum().nlargest(n)
    bottom_mention_users = df.groupby('user')['mentions'].sum().nsmallest(n)
    
    # Top and bottom 10 users by message count
    top_message_users = df.groupby('user')['message_count'].sum().nlargest(n)
    bottom_message_users = df.groupby('user')['message_count'].sum().nsmallest(n)

    # Top and bottom 10 users by reaction count
    top_reaction_users = df.groupby('user')['reactions'].sum().nlargest(n)
    bottom_reaction_users = df.groupby('user')['reactions'].sum().nsmallest(n)
    
    # Top 10 messages by replies
    top_messages_by_replies = df.nlargest(n, 'replies')

    # Top 10 messages by reactions
    top_messages_by_reactions = df.nlargest(n, 'reactions')

    # Top 10 messages by mentions
    top_messages_by_mentions = df.nlargest(n, 'mentions')

    # Channel with the highest activity
    channel_with_highest_activity = df.groupby('channel').sum()[column].idxmax()

    # Channel appearing at the right top corner in the scatter plot
    scatter_plot_channel = df.groupby('channel').agg({'message_count': 'sum', 'replies': 'sum', 'reactions': 'sum'}).idxmax().values[0]

    # Fraction of messages replied within the first 5 minutes
    fraction_replied_within_5mins = len(df[df['reply_time'] <= 5]) / len(df)


def top_users_by_count(df, column, n=10, ascending=False):
    """Get the top users by count for a specific column."""
    top_users = df.groupby('sender_name')[column].sum().sort_values(ascending=ascending).head(n)
    return top_users


def top_messages_by_count(df, column, n=10, ascending=False):
    """Get the top messages by count for a specific column."""
    top_messages = df.sort_values(by=column, ascending=ascending).head(n)
    return top_messages

def channel_with_highest_activity(df):
    """Get the channel with the highest activity."""
    channel_activity = df['channel'].value_counts()
    channel_with_highest_activity = channel_activity.idxmax()
    return channel_with_highest_activity

def fraction_of_messages_replied_within_time(df, time_threshold):
    """Calculate the fraction of messages replied within a specified time threshold."""
    replied_messages = df[~df['reply_users'].isnull()]
    replied_within_threshold = replied_messages[replied_messages['msg_sent_time'] - replied_messages['thread_ts'] <= pd.Timedelta(minutes=time_threshold)]
    fraction_replied = len(replied_within_threshold) / len(df)
    return fraction_replied

# Scatter plot of time difference and time of the day
    plt.scatter(df['reply_time'], df['time_of_day'], c=df['channel'])
    plt.xlabel('Time difference between message and first reply (minutes)')
    plt.ylabel('Time of the day (24hr format)')
    plt.title('Scatter plot of Time Difference and Time of the Day')
    plt.colorbar(label='Channel')
    plt.show()






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')

    
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
