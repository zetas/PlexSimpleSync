import argparse
import itertools
import time
import random

from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound, BadRequest
from plexapi.library import MovieSection, ShowSection
from plexapi.video import Video
from plexapi.utils import threaded

from pyfiglet import Figlet
from tqdm import tqdm

from multiprocessing.dummy import Pool as ThreadPool

start_time = time.time()

parser = argparse.ArgumentParser(
    description='Sync watched status on multiple plex servers and libraries.')
parser.add_argument('username', help='Plex.tv Username')
parser.add_argument('password', help='Plex.tv Password')
parser.add_argument('-d', '--dryrun', action='store_true',
                    help='Perform a dry run. Don\'t make changes, just list what will change')
parser.add_argument('-v', '--verbose', action='store_true',
                    help='Verbose output')
requiredOpts = parser.add_argument_group('required named arguments')
requiredOpts.add_argument('--libraries', nargs='*',
                          help='Plex libraries you want to sync. Use quotes to contain spaces, i.e, --libraries "TV Shows". Note: They must be named the same across all sync\'d servers!!', required=True)
requiredOpts.add_argument('--servers', nargs='*',
                          help='Plex server names.', required=True)

args = parser.parse_args()

username = args.username
password = args.password
libraries = args.libraries
servers = args.servers
dryrun = args.dryrun
verbose = args.verbose


def markWatched(args):
    movie = args[0]
    pbar = args[1]
    # for arg in args:
    #     if isinstance(arg, Video):
    #         movie = arg
    #     elif isinstance(arg, tqdm):
    #         pbar = arg
    if movie is not None and pbar is not None:
        pbar.update(1)
        movie.markWatched()
        return 1
    
    return 0


def sync_movies(masterLibrary, master, slave, library):
    syncResult = []
    fullContent = masterLibrary.search(unwatched=False)

    masterWatchedGUIDs = []

    for content in fullContent:
        masterWatchedGUIDs.append(content.title)

    slaveWatchedContent = slave.library.section(
        library).search(unwatched=False)
    slaveUnwatched = {}
    slaveWatchedGUIDs = []

    for content in slaveWatchedContent:
        slaveWatchedGUIDs.append(content.title)

    s = set(slaveWatchedGUIDs)
    updateContentTitles = [x for x in masterWatchedGUIDs if x not in s]

    if len(updateContentTitles) > 0:
        slaveUnwatchedContent = slave.library.section(
            library).search(unwatched=True)
        for content in slaveUnwatchedContent:
            slaveUnwatched[content.title] = content
        toMarkWatched = []
        with tqdm(total=len(updateContentTitles)) as pbar:
            for title in updateContentTitles:
                slaveContent = slaveUnwatched.get(title)
                if (slaveContent is None):
                    if verbose:
                        pbar.write('{} not found on {}!'.format(
                            title, slave.friendlyName))
                    pbar.update(1)
                    continue
                if slaveContent.viewCount == 0:
                    if verbose or dryrun:
                        pbar.write('{} is unwatched on {}!'.format(
                            title, slave.friendlyName))
                    if not dryrun:
                        toMarkWatched.append([slaveContent, pbar])
                        # if verbose:
                        #     pbar.write('{} marked as watched on {}.'.format(title,slave.friendlyName))
                        #     pbar.write('--------')
                    # syncedCount += 1
            if verbose:
                pbar.write('Batch processing unwatched files...')
            # syncResult = threaded(markWatched, toMarkWatched)
            pool = ThreadPool()
            syncResult = pool.map(markWatched, toMarkWatched)  
            pool.close()
            pool.join()
        # print(syncResult)

    return sum(syncResult)

def updateGuid(*args, **kwargs):
    guids = []
    argList = list(args)

    currentIteration = argList.pop()
    returnVals = argList.pop()

    for arg in argList:
        guids.append(arg[0].guid)
        arg[1].update(1)

    returnval[i] = guids
    # if job_is_done_event:
    #     pbar.update(1)
    

def sync_tv(masterLibrary, master, slave, library):
    # key = '/library/sections/{}/all?type=4&viewCount>=0'.format(masterLibrary.key)
    # fullContent = masterLibrary.fetchItems(key)

    fullContent = masterLibrary.searchEpisodes(unwatched=False)
    syncResult = []
    masterWatchedGUIDs = []

    with tqdm(total=len(fullContent)) as pbar:
        pbar.write('Compiling list of watched content on master...')
        for content in fullContent:
             masterWatchedGUIDs.append([content, pbar])

        l = masterWatchedGUIDs
        n = 50
        threadWork = [l[i:i + n] for i in range(0, len(l), n)]
        threaded(updateGuid, threadWork)
        # pool = ThreadPool(8)
        # syncResult = pool.map(updateGuid, masterWatchedGUIDs)  
        # pool.close()
        # pool.join()

    print(vars(syncResult))
    return 0

    slaveWatchedContent = slave.library.section(library).searchEpisodes(unwatched=False)
    slaveWatchedGUIDs = []
    slaveUnwatched = {}

    with tqdm(total=len(slaveWatchedContent)) as pbar:
        pbar.write('Compiling list of watched content on slave...')
        for content in slaveWatchedContent:
            slaveWatchedGUIDs.append(formatString) 
            pbar.update(1)

    s = set(slaveWatchedGUIDs)
    updateContentGUIDs = [x for x in masterWatchedGUIDs if x not in s]

    if len(updateContentGUIDs) > 0:
        slaveUnwatchedContent = slave.library.section(library).search(unwatched=True)
        for content in slaveUnwatchedContent:
            slaveUnwatched[content.guid] = content
        toMarkWatched = []
        with tqdm(total=len(updateContentGUIDs)) as pbar:
            for guid in updateContentGUIDs:
                slaveContent = slaveUnwatched.get(guid)
                if (slaveContent is None):
                    if verbose:
                        pbar.write('{} not found on {}!'.format(
                            guid, slave.friendlyName))
                    pbar.update(1)
                    continue
                if slaveContent.viewCount == 0:
                    if verbose or dryrun:
                        pbar.write('{}:{} - {} is unwatched on {}!'.format(
                            slaveContent.grandparentTitle, slaveContent.seasonEpisode, slaveContent.title, slave.friendlyName))
                    if not dryrun:
                        toMarkWatched.append([slaveContent, pbar])
                        # if verbose:
                        #     pbar.write('{} marked as watched on {}.'.format(title,slave.friendlyName))
                        #     pbar.write('--------')
                    # syncedCount += 1
            if verbose:
                pbar.write('Batch processing unwatched files...')
            # syncResult = threaded(markWatched, toMarkWatched)
            if len(toMarkWatched) > 0:
                pool = ThreadPool()
                syncResult = pool.map(markWatched, toMarkWatched)  
                pool.close()
                pool.join()
    return sum(syncResult)


    # syncedCount = 0
    # masterContent = masterLibrary.searchEpisodes(unwatched=False)
    # with tqdm(total=len(masterContent)) as pbar:
    #     for content in masterContent:
    #         try:
    #             slaveContent = slave.library.section(library).get(content.grandparentTitle).episode(
    #                 season=content.seasonNumber, episode=content.index)
    #         except (NotFound, BadRequest):
    #             if verbose:
    #                 pbar.write('Episode {} of show {} not found on {}. Searching by title instead...'.format(
    #                     content.title, content.grandparentTitle, slave.friendlyName))
    #             try:
    #                 slaveContent = slave.library.section(library).get(
    #                     content.grandparentTitle).episode(content.title)
    #                 if verbose:
    #                     pbar.write('Found by title: ({}-{}:{}) ({}-{}:{})'.format(
    #                         master.friendlyName,
    #                         content.grandparentTitle,
    #                         content.title,
    #                         slave.friendlyName,
    #                         slaveContent.grandparentTitle,
    #                         slaveContent.title))
    #             except (NotFound, BadRequest):
    #                 if verbose:
    #                     pbar.write('Unable to find slave episode by master title or season/episode: {} {}'.format(
    #                         content.seasonEpisode,
    #                         content.title))
    #                 pbar.update(1)
    #                 continue

    #         if slaveContent.viewCount == 0:
    #             if verbose or dryrun:
    #                 pbar.write('{} is watched on {} but not on {}.'.format('{}:{}'.format(
    #                     content.grandparentTitle, content.title), master.friendlyName, slave.friendlyName))
    #                 pbar.write('GUIDS: Master: {} Slave: {}'.format(content.guid, slaveContent.guid))
    #             if not dryrun:
    #                 slaveContent.markWatched()
    #                 if verbose:
    #                     pbar.write('{} marked as watched on {}.'.format('{}:{}'.format(
    #                         content.grandparentTitle, content.title), slave.friendlyName))
    #                     pbar.write('--------')
    #             syncedCount += 1
    #         pbar.update(1)
    # return syncedCount


def sync_servers(master, slave, library):
    syncedCount = 0
    try:
        masterLibrary = master.library.section(library)
    except NotFound:
        print('Library {} not found on server {}. Make sure you include the full library name, i.e. TV Shows. It\'s case sensitive.'.format(
            library, master.friendlyName))
        return False

    if isinstance(masterLibrary, MovieSection):
        syncedCount = sync_movies(masterLibrary, master, slave, library)
    elif isinstance(masterLibrary, ShowSection):
        syncedCount = sync_tv(masterLibrary, master, slave, library)
    else:
        print('Library {} is not a TV or Movie library. We only currently support those library types.')
        return False

    print('Synced {} items'.format(syncedCount))
    print('################')


account = MyPlexAccount(username, password)

f = Figlet(font='slant')
print(f.renderText('Plex Sync'))

serverList = {}
for server in servers:
    if verbose:
        print('Connecting to {}...'.format(server))
    serverList[server] = account.resource(server).connect()

for library in libraries:
    for serverProduct in itertools.product(servers, repeat=2):
        if serverProduct[0] != serverProduct[1]:
            print('Syncing {} library from {} to {}...'.format(
                library, serverProduct[0], serverProduct[1]))
            sync_servers(serverList[serverProduct[0]],
                         serverList[serverProduct[1]], library)

print('Sync\'d libraries in {:.2f} seconds.'.format(time.time() - start_time))
