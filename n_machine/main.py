'''
otacon: Extracts Reddit comments from the offline version of the Pushshift Corpus dumps (see README for further info)

Usage:
Basic command:
poetry run python path/to/otacon/otacon/main.py

Required args:
--input or -I: path to directory containing the Pushshift data dumps
--output or -O: desired output path

Optional args: (supplying at least one of them is however required)
--time-from or -F: earliest month to extract from in YYYY-MM format
--time-to or -T: latest month to extract form in YYYY-MM format
--src or -S: source to extract from, either "subreddit" or "user"
--name or -N: name of the source to extract from
--regex or -R: regular expression to use for matching
--popularity or -P: minimum voting score threshold for extracted comments
--toplever or -TL: only extract top-level comments

Soft filters for:
profanity
bot-generated comments

Hard filters for:
Regex match inside a quoted line
Duplicates

Output:
CSV file with search parameters and time of execution in the filename.
Includes span (for regex matches), subreddit, score, user, flairtext, date, and permalink as metadata.
Soft-filtered comments are included in a separate file with their respective filtering reason.

'''

import os
import re
import csv
import json
import pickle
import random
import logging
import calendar
import argparse
from datetime import datetime
from typing import TextIO
import pandas as pd

from zstandard import ZstdDecompressor

# keep track of already-processed comments throughout function calls
hash_list = []

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}

pronouns_path = os.path.expanduser("~/Documents/GitHub/pronounlist/Pronouns")
pronouns = list()
for root, dirs, files in os.walk(pronouns_path):
	for file in files:
		if not file.startswith('.'):
			filepath = os.path.join(root, file)
			with open(filepath) as infile:
				prons = infile.read().split('\n')
				for elem in prons:
					pronouns.append(elem)

# dedupe
pronouns = list(set(pronouns))
# sort by length to ensure correct matching
pronouns = sorted(pronouns, key=lambda x: len(x), reverse=True)


pronouns_bars = '|'.join(pronouns)
pronouns_regex = f'(?:{pronouns_bars})'
pronouns_regex = f'\\b{pronouns_regex}/{pronouns_regex}\\b'

free_pronoun_regex = '(.+)/\\2s(elf)?\\b'

noanyall_regex = "(no|any|all).pronouns?"

combined_regexes = '|'.join([pronouns_regex, free_pronoun_regex, noanyall_regex])
combined_negative_regex = f'(?:({combined_regexes}))'

# comparison data
path = "./assets/pronoun_declarers.pkl"
with open(path, "rb") as infile:
    declarers = pickle.load(infile)


# Results data
subs = ['asktransgender', 'languagelearning', 'traaaaaaannnnnnnnnns', 'ftm', 'transgamers', 'NonBinary', 'LGBT_Muslims', 'motorcycles', 'ChapoTrapHouse', 'giftedstudents', 'traaNSFW', 'badunitedkingdom', 'JustNoTalk', 'pansexual', 'weddingplanning', 'aaaaaaacccccccce', 'crankthatfrank', 'actuallesbians', 'agender', 'EnbyFashionAdvice', 'MtF', 'bisexual', 'asexuality', 'transgendercirclejerk', 'COMPLETEANARCHY', 'ennnnnnnnnnnnbbbbbby', 'CapitalismVSocialism', 'transfurs', 'BorderlinePDisorder', 'GenderCritical', 'tumblr', 'lgbt', 'transtimelines', 'TumblrInAction', 'ffxiv', 'Cumtown', 'transgender', 'TwoXChromosomes', 'transtwincities', 'TransyTalk', 'Gamingcirclejerk', 'DotA2', 'trans', 'UCI', 'ask_transgender', 'genderfluid', 'transpositive', 'FFRecordKeeper', 'EngineeringStudents', 'LGBTeens', 'TryingForABaby', 'GaymersGoneMild', 'stupidpol', 'pcmasterrace', 'Warthunder', 'ABraThatFits', 'GenderCynical', 'fatlogic', 'genderqueer', 'ainbow', 'GirlGamers', 'TransChristianity', 'TransSpace', 'rit', 'AskAcademia', 'Scriptcraftguild', 'stevens', 'Guildwars2', 'Autos', 'cars', 'speedrun', 'transgenderUK', 'DBZDokkanBattle', 'feminineboys', 'proED', 'flying', 'grandorder', 'berkeley', 'AprilBumpers2018', 'ProtectAndServe', 'AirForce', 'pathofexile', 'shaving', 'transpassing', 'FTMOver30', 'hogwartswerewolvesB', 'fallenlondon', 'milliondollarextreme', 'yugioh', 'polyamory', 'TranslationStudies', 'Cameras', 'PSO2', 'birthcontrol', 'engineering', 'FULLCOMMUNISM', 'MakeupAddiction', 'curlyhair', 'u_TheEdenCrazy', 'AskFeminists', 'fo4', 'livesound', 'bois', '911dispatchers', 'MonsterHunter', 'Guitar', 'conlangs', 'TransMakeupHeaven', 'italianlearning', 'GaySoundsShitposts', 'Polska', 'Vaping101', 'Vaping', 'electronic_cigarette', 'transadorable', 'furry', 'TransLater', 'goodyearwelt', 'Kindred', 'TrollXChromosomes', 'gaybros', 'Planetside', 'PuzzleAndDragons', 'LegalAdviceUK', 'muacirclejerk', 'AskEngineers', 'TeraOnline', 'TiADiscussion', 'medicine', 'askGSM', 'RWBY', 'FemmeThoughts', 'raisedbynarcissists', '4ChanMeta', 'MECoOp', 'centralmich', 'CasualConversation', 'OkCupid', 'Rolla', 'badlinguistics', 'TumblrAtRest', 'TheLastAirbender', 'selfharm', 'ShitRedditSays', 'Embroidery', 'pebble', 'Drexel', 'RoyaleHighTrading', 'crosstradingrblx', 'BDSMnot4newbies', 'Cross_Trading_Roblox', 'AntiHateCommunities', 'Grimdank', 'camaro', 'TheMotte', 'bigdickproblems', 'SpoiledDragRace', 'RomanceClub', 'TwoBestFriendsPlay', 'egg_irl', 'lgballt', 'UUreddit', 'VoteDEM', 'TransTryouts', 'NonBinaryTalk', 'TheSantaAnaWinds', 'FTM_SELFIES', 'malaysia', 'DankLeft', 'HogwartsWerewolves', 'ADMU', 'BisexualTeens', 'mendrawingwomen', 'SapphoAndHerFriend', 'aromanticasexual', 'okbuddyhetero', 'unpopularopinion', 'askphilosophy', 'muacjdiscussion', 'ostomy', 'LabourUK', 'sterilization', 'AskAsexual', 'Healthyhooha', 'honesttransgender', 'traandwagon', 'badwomensanatomy', 'fullegoism', 'MoneyDiariesACTIVE', 'truscum', 'transgenderau', 'Anarchism', 'demigirl_irl', 'teengirlswholikegirls', 'SmolBeanSnark', 'snails', 'Therian', 'ActualPublicFreakouts', 'bropill', 'biromantic', 'GachaLifeCringe', 'PokemonMasters', 'GamerGhazi', 'intuitiveeating', 'questioning', 'CoronavirusCirclejerk', 'neoconNWO', 'golf', 'Lgbtforestkids', 'neurodiversity', 'popheads', 'FTMfemininity', 'dankmemes', 'DemiBoy', 'DIDmemes', 'MadeOfStyrofoam', 'LosAngeles', 'PiercedCock', 'Rollerskating', 'blogsnark', 'hogwartswerewolvesA', 'FixedGearBicycle', 'teenagersnew', 'smashbros', 'DIYemo', 'FTMMen', 'NoFeeAC', 'osugame', 'WLWs_and_Wyrms', 'bi_irl', 'childfree', 'homestuck', 'snapcube', 'EnbyLewds', 'me_irlgbt', 'ACNHTrade', 'AreTheCisOk', 'NBA2k', 'okbuddyableist', 'askgaybros', 'AdoptMeTrading', 'lgbtmemes', 'ftmspunished', 'OpenChristian', 'Destiny', 'DissidiaFFOO', 'Nonbinaryteens', 'phallo', 'neoliberal', 'Lewd', 'forhonor', 'PinkEnts', 'omnisexual', 'PlusSize', 'PrideFlags', 'Twitch', 'micronations', 'starcitizen', 'TransIreland', 'comingout', 'multilingualparenting', 'malefashion', 'nus', 'PupPlay', 'exmormon', 'TheGirlSurvivalGuide', 'RadicallyOpenDBT', 'Asexual', 'fakebaseball', 'Megaman', 'QueerVexillology', 'TransVent', 'HalosTheRoleplay', 'AskAromantics', 'automationgame', 'videography', 'DID', 'aromantic', 'GatekeepingYuri', 'Choices', 'foreverbox', 'DIDart', 'europeanunion', 'DestinyTheGame', 'gaytransguys', 'transnord', 'selfharmteens', 'cataclysmdda', 'NoStupidQuestions', 'uofmn', 't4t', 'Infiniteaxesmemes', 'FtMPorn', 'transnames', 'WholesomeTeenBoys', 'SexOnTheSpectrum', 'Reduction', 'VaushV', 'loseit', 'WitchesVsPatriarchy', 'MakeupLounge', 'chastity', 'TransAdoption', 'demisexuality', 'lgballtanarchy', 'aoe2', 'AskMen', 'tranimemes', 'BugFables', 'LesbianGamers', 'GayChristians', 'asiantransgender', 'MHEIAEd_BookStudy', 'transgenderteens', 'germantrans', 'EDanonymemes', 'trans_fat', 'RoleReversal', 'modular', 'BokuNoShipAcademia', 'APStudents', 'FFBraveExvius', 'DBDGoneGay', 'Transpies', 'sto', 'TheTCC', 'massage', 'Periods', 'DIDCringe', 'voidpunk', 'BeautyGuruChatter', 'gayjews', 'communism101', 'nerdfighters', 'tall', 'weddingyuri', 'entp', 'GradSchool', 'salmacian', 'EuropeanFederalists', 'nudism', 'DarkFics', 'iacceptyou', 'tarot', 'papermario', 'bigboobproblems', 'butchlesbians', 'Negareddit', 'hotleft', 'Jreg', 'exjw', 'TTC30', 'Undertale', 'Invisaforce', 'truechildfree', 'TrollCoping', 'JellesMarbleRuns', 'unpopularkpopopinions', 'Slovakia', 'masstagger', 'FreedTheNips', 'beautytalkph', 'subnautica', 'rainbowcoven', 'translator', 'atwwdpodcast', 'Doom', 'CarletonU', 'surrealNBmemes', 'TopMindsOfReddit', 'PostTransitionTrans', 'WPI', 'adhdmeme', 'Pride_and_Positivity', 'TransMasc', 'insaneparents', 'infj', 'TheSilphRoad', 'HollowKnight', 'DualGender', 'LateStageCapitalism', 'transbooks', 'Tokophobia', 'transhumanism', 'menstrualcups', 'roblox', 'styrofashion', 'twittermoment', 'XboxSeriesX', 'otherkin', 'actual_detrans', 'ObeyMeNSFW', 'lgbtchoicesfans', 'bemorechill', 'SuddenlyTrans', 'MasculineOfCenter', 'pregnant', 'transvoice', 'socialism', 'otomegames', 'BlakeTheFemboy', 'mylittlepony', 'onejoke', 'ShitLiberalsSay', 'phlgbt', 'linuxmasterrace', 'antiwork', 'Markiplier', 'transbookclub', 'SatisfactoryGame', 'pokemongo', 'ToiletPaperUSA', 'Grej', 'KailahsCringe', 'Winkerpack', 'CrankGameplays', 'wallstreetbets', 'tsuyonpu', 'polyfamilies', 'TransBuyAndSell', 'SocialistRA', 'EatingIntuitively', 'crossdressing', 'ExPentecostal', 'AdptmeAndRhTrading', 'RPI', 'AskDID', 'shittylgbt', 'Eurosceptics', 'Fixxit', 'RedditForGrownups', 'mitski', 'mentalhealth', 'HaveWeMet', 'CalPoly', 'transteens', 'music_survivor', 'GachaClub', 'asktransmen', 'FTMFitness', 'EDAnonymous', 'adultsnew', 'jeanshate', 'mypartneristrans', 'YUROP', 'SimDemocracy', 'RHcosplayCommunity', 'traadustCrusaders', 'TERFisafetish', 'FundieSnarkUncensored', 'Archery', 'Persona5', 'MyChemicalRomance', 'EternalCardGame', 'uwaterloo', 'alltheleft', 'femby', 'SwordPansexual', 'latterdaysaints', 'synthesizers', 'LateStageImperialism', 'ThomasDoesMath', 'religion', '2007scape', 'lowspecgamer', 'lesbianfashionadvice', 'LGBTARMY', 'MtFteens', '8ValuesMemes', 'short', 'europe', 'TransCommunity', 'DemocratsforDiversity', 'RoyaleHigh_Trading', 'cottagecore', 'Vanderbilt', 'WingsOfFire', 'leagueoflegends', 'swtor', 'GachaUwU', 'INTP', 'writerchat', 'OkBuddyCatgirl', 'FanFiction', 'projectcar', 'MTFsPunished', 'SequelMemes', 'thegooddoctor', 'CPTSDmemes', 'ChurchOfWaterLaw', 'IronFrontUSA', 'celestegame', 'shiftingrealities', 'Marxism', 'MakeNewFriendsHere', 'BPD', 'ffacj', 'AskEnbies', 'SanAntonioLGBTQIA', 'F1NN5TER', 'RustyQuill', 'SocialDemocracy', 'astrologymemes', 'astrology', 'neocentrism', 'queermakeup', 'terfsplaining', 'CHAZRevolution', 'CanadaPolitics', 'AskAnAmerican', 'HogwartsGhosts', 'RoyaleHighTrading__', 'wormswithlegs', 'Cervix', 'InclusiveMenstruation', 'RoyaleHigh_Artt', 'EliteDangerous', 'WeAreSorry', 'LateStageGenderBinary', 'autism', 'redditmoment', 'suddenlybi', 'AceTeens', 'Jewish', 'plural', 'BB30', 'THE_PACK', 'Logic_Studio', 'GachaUnity', 'pagan', 'circlebroke2', 'DemiFemme', 'RPGdesign', 'Dermatillomania', 'txstate', 'royalehighroleplay', 'TransDIY', '6thForm', 'queerfashions', 'menstruation', 'KarlJacobs', 'ftmtimelines', 'panromantic', 'rblxtrading_', 'Christianity', 'goodanimemes', 'BDSMcommunity', 'HalfLife', 'RivalsOfAether', 'arttocope', 'transontario', 'LesbianActually', 'SexWorkersOnly', 'OUTFITS', 'queergonewildstories', 'TaylorSwift', 'badreligion', 'siyoungiyo', 'IWantOut', 'plural_irl', 'peopleofwalmart', 'queerception', 'dreamnotfound', 'lgbtqia_poc', 'Cubers', 'Overwatch', 'SIFallstars', 'Dragula', 'exchristian', 'cancer', 'magicthecirclejerking', 'mildlyinfuriating', 'MechanicalEngineering', 'MaleEatingDisorders', 'RblxDrawing', 'Proust', 'amateurradio', 'AntifascistsofReddit', 'BisexualMen', 'AnarchismZ', 'PhonesAreBad', 'toddlers', 'dirtbagcenter', 'QContent', 'podcasting', 'OKState', 'analog', 'Skookum', 'neopets', 'Rainbow6', 'MonsterHunterWorld', 'TowerOfBabylon', 'GCdebatesQT', 'guitarcirclejerk', 'duolingo', 'SRSsucks', 'Brawlhalla', 'civ', 'TumblrPls', 'college', 'UCSC', 'fuckeatingdisorders', 'diyelectronics', 'pokemon', 'SailorMoonDrops', 'Beto2020', 'philadelphia', 'malingering', 'lfg', 'Drama', 'PrincessesOfPower', 'AskDocs', 'Stims', 'knifeclub', 'harrypotter', 'MechanicalKeyboards', 'Purdue', 'SubredditDrama', 'transalute', 'wedding', 'solotravel', 'knives', 'MHOC', 'NintendoSwitch', 'DQBuilders', 'ENLIGHTENEDCENTRISM', 'fursuitsex', 'germany', 'transgeneral', 'AskElectronics', 'Battlefield', 'MTU', 'sjwhate', 'slashdiablo', 'Fallout', 'ccna', 'illnessfakersgonewild', 'poledancing', 'TheArcana', 'stilltrying', 'Outlander', 'BGCCircleJerk', 'Parahumans', 'TrueSTL', 'Rockband', 'stevenuniverse', 'harrystyles', 'NorthCarolina', 'BabyBumps', 'BikiniBottomUnion', 'twilight', 'ABBACircleJerk', 'nb_mp', 'CPTSD', 'MysteryDungeon', 'shadowpeople', 'TalesFromTheCustomer', 'DebateCommunism', 'universityofauckland', 'GenZanarchist', 'Connieverse', 'GH5', 'CrimsonNecklace', 'infp', 'Vaporwave', 'talesfromtechsupport', 'transprogrammer', 'ArtCrit', 'auntienetwork', 'PopHeadsGossip', 'SNSD', 'WVU', 'sse', 'UPenn', 'FoxStevenson', 'TumblrCirclejerk', 'Anarcho_Capitalism', 'FtMpassing', 'Tulpas', 'gigantic', 'VirginiaTech', 'Warframe', 'SwitchHacks', 'aviation', 'Acceleracers', 'SwitchHaxing', 'OpenUniversity', 'UrinatingTree', 'LoveNikki', 'Temple', 'nonbinaryUK', 'TransgenderIncels', 'pokemonshowdown', 'rupaulsdragrace', 'transeducate', 'BestOfOutrageCulture', 'unixporn', 'ModelResources', 'SkincareAddiction', 'BMW', 'UCDavis', 'DunderMifflin', 'SF4', 'ACTrade', 'offmychest', 'OKCupidLGBT', 'TagProIRL', 'GenderCriticalTheory', 'cscareerquestions', 'MrRobot', 'pyrocynical', 'supergirlTV', 'ukpolitics', 'churning', 'shittyMBTI', 'ModernMagic', 'summonerschool', 'spongebob', 'arrow', 'MHOCPress', 'fea', 'justlegbeardthings', 'EdAnonymousAdults', 'Gachatardhaven', 'feedthebeast', 'polandball', 'LGBTeensDate', 'transpoc', 'AskTeenGirls', 'NonBinaryGamers', 'SexPositiveNofap', 'PlasticGuitarSurgeon', 'transED', 'StarKid', 'detrans', 'Deltarune', 'skinnygossip', 'petplay', 'SocialistGaming', 'SnepYiff', 'shitfascistssay', 'BDSMGW', 'language_exchange', 'Lovestruck', 'schooloftrans', 'transmasculine', 'vegan', 'IBO', 'iastate', 'brisbane', 'flightsim', 'headphones', 'HeadphoneAdvice', 'NotHowGirlsWork', 'radicalqueers', 'AmIASexyQueer', 'MHOCStormont', 'villagetownsquare', 'vaginismus', 'lgbtcirclejerk', 'BreadTube', 'JudgeMyAccent', 'enfj', 'Nexus', 'MillerPlanetside', 'ems', 'KCRoyals', 'halo', 'PurplePillDebate', 'TalesFromYourServer', 'the1975', 'PokemonTCG', 'genderfluid_irl', 'MHOCStrangersBar', 'FreeBeauty', 'youngadults', 'vegancirclejerk', 'SBU', 'sciencesays1gender', 'PragerUrine', 'rutgers', 'gaybroscirclejerk', 'beholdthemasterrace', 'LethalLeague', 'queerean', 'dpdr', 'Futurology', 'French', 'volleyball', 'UIUC', 'whatsthisbug', 'SkyrimPorn', 'DragonAgeCoOp', 'shittyfalloutlore', 'xboxone', 'Enough_Sanders_Spam', 'highschoolfootball', 'Puberty', 'TraaButOnlyBees', 'Anxiety', 'TheGoodPlace', 'Drag', 'gamegrumps', 'queerwitches', 'MakeMeSuffer', 'waifuism', 'CSULB', 'AccidentalPrideFlags', 'Fishing', 'rpdrcirclejerk', 'polyglot', 'traversecity', 'skyrimrequiem', 'German', 'gqfitness', 'bardmains', 'PokkenGame', 'XenogendersAndMore', 'socdemnetwork', 'CuratedTumblr', 'yeagerbomb', 'Competitiveoverwatch', 'Kirby', 'RoyaleHigh_Giveaway', 'AliceOseman', 'CrossTrading_inRoblox', 'neopronouns', 'RoyaleHigh_CrossTrade', 'RobloxCrosstrading', 'ConservativeKiwi', 'EnbyandChill', 'tankiejerk', 'teenagers', 'Hasan_Piker', 'cripplingalcoholism', 'RightJerk', 'MarvelStudiosSpoilers', 'queensofleague', 'wallstreetbetsHUZZAH', 'satanism', 'RoyalHighTradingHub', 'TransandroSupport', 'PansexualTeens', 'asklatinamerica', 'electricvehicles', 'dreamgender', 'Genshin_Impact', 'midsize', 'IBEW', 'AskStudents_Public', 'AssCredit', 'PokemonROMhacks', 'SASSWitches', 'Paramore', 'BattleAxeBisexualVibe', 'Greysexuality', 'discworld', 'CROSSTRADINGrh', 'RobloxRoyaleHigh_', 'LGBTQ_Safe_Space', 'UofT', 'ABoringDystopia', 'Chodi', 'notlikeothergirls', 'TheOwlHouse', 'proudlibtard', 'speculum', 'animecirclejerk', 'RobloxCommissions', 'Speciesdysphoria', 'u_Angie-The-Rat', 'FTMPornNoHole', 'teenagersgaming', 'NoRulesCalgary', 'lunarcraft', 'MultipleSclerosis', 'notliketheothergirls', 'TheQueerKiwi', 'selfharm_memes', 'cross_trading_robIx', 'trixic', 'LibJerk', 'SimsMobile', 'ClubChubPositivity', 'ForzaHorizon', 'GuildWars', 'Lal_Salaam', 'straykids', 'PositiveArtReviews', 'emotionalabuse', 'Lovelink', 'WroteAThing', 'PMDD', 'QueerStem', 'tickling', 'wholesomejojo', 'traaaaaaarrrrrrrro', 'vancouver', 'Evasexual', 'Cringetopia', 'always_lsg', 'gachacoolers', 'leftistvexillology', 'omni_teens', 'Insurance', 'greenday', 'EdgingTalk', '196', 'speedofliberals', 'LilyIsTrans', 'DanganronpaCringe', 'ControversialOpinions', 'RoyaleHigh_Roblox', 'titanfall', 'tokipona', 'QueerClimbers', 'demiromantic', 'WastedGachaTalent', 'arknights', 'nonbinarylesbians', 'LGBTQCORNER', 'DemiboyCulture', 'eddit', 'Bigender_irl', 'CountryHumans', 'RadicalChristianity', 'CountryhumansCringe', 'u_darlinqvera', 'BlakedAlaska', 'Orientedaroace', 'traabutyescommies', 'BisexualHumans', 'GothStyle', 'toxicparents', 'destiny2', 'melanatedandmodified', 'thebearbubb', 'SexyDimorphism', 'Trading_RH', 'ddlg', 'qatar', 'GlobalTalk', 'deadbydaylight', 'mixedrace', 'Intactivism', 'disenchantment', 'WTT_graduates', 'intersex', 'DreamWasTaken2', 'knitting', 'RobloxMiddleMan', 'TokyoAfterschool', 'SonicTheHedgehog', 'splatoon', 'cruze', 'UnderworldOffice', 'cfs', 'yurimemes', 'BPDmemes', 'starbound', 'RomanceBooks', 'asexualteens', 'traaaaaaarrrrrrrroace', 'thomastheplankengine', 'xxsurfing', 'namenerds', 'dndnext', 'BuddyCrossing', 'Unitale', 'AntiGachasCringe', 'TwoXSupport', 'Aritzia', 'BrujeriaEnglish', 'prochoice', 'xkcd', 'GachaVenting', 'TransClones', 'TruTalk', 'Megaten', 'Anglicanism', 'TAZCirclejerk', 'fakedisordercringe', 'Unexpected', 'FantasyToy', 'poetry_critics', 'TransTeensMeet', 'INFPmemes', 'RoyaleHigh_Refunds', 'IncelTear', 'Cumrades', 'PleaseCallMeRedScarf', 'CoiningTerms', 'Crosstradesxroblox', 'lostgeneration', 'billycobb', 'facepalm', 'crosstrading_rblxxx', 'AnimalCrossing', 'teenboys', 'mbtimemes', 'mysticmessenger', 'FDS_is_Transphobic', 'LeftistLGBTMemes', 'AreTheTransOkay', 'forcedbreeding', 'IZ_ILUV', 'Libertarian', 'WineEP', 'cross_trading_roblocs', 'SandersSides', 'GachaEdits', 'AGPAAPmemes', 'GachaClubCringe', 'CollegeRant', 'lesbianteens', 'feedthetraa', 'femalefashion', 'Toonami', 'ApexUncovered', 'houkai3rd', 'VeganForCircleJerkers', 'asexualdating', 'AskGirls', 'playstation', 'AngelSanctuary', 'AttackOnRetards', 'NoRules', 'QueerSimmers', 'obeyme', 'ABDL', 'LGBTQquestions', 'menstruationstation', 'enbynsfw', 'lgbthistory', 'MakeupRehab', 'DDLC', 'periods_for_all', 'BeachCity', 'StarVStheForcesofEvil', 'pawsomeanimals', 'aarrrooooooaaacceeeee', 'CreepyWikipedia', 'fragileancaps', 'kitseckshouse', 'askhotels', 'blackladies', 'Wattpad', 'EnoughCommieSpam', 'martialarts', 'AroAceAgender', 'Collegeauditions', 'yaoi', 'subFoxo7002', 'earnrobux', 'Mtf_irl', 'pcgaming', 'transdating', 'pan_irl', 'TalesFromLGBT', 'overclocking', 'transgendervegan', 'hbomberguy', 'AntiLGBTQ', 'Animemes', 'touhou', 'TransPunished', 'Polandballart', 'RPGcreation', 'bigonewild', 'jregHot', 'anarchocommunism', 'LollyBopSong', 'Bringbackdragons', 'pokemontrades', 'ShrugLifeSyndicate', 'TransRoachCult', 'PCOS', 'CanadaPride', 'BobDylanCircleJerk', 'DuelLinks', 'LGBTQ_AnimalCrossing', 'rollercoasters', 'DemiGirl', 'heart_stopper', 'oddlysatisfying', '0sanitymemes', 'MaladaptiveDreaming', 'ScienceTeachers', 'DebateAnarchism', 'OSU', 'ArosShow', 'lucifer', 'GentleDungeon', 'ThePortlandLeft', 'PolskaPolityka', 'FundieSnark', 'SyrianCirclejerkWar', 'CornGuy', 'ainbowregion', 'AnarchoWave', 'theroyalrealms', 'RHPhotoshoots', 'u_rh_bunny', 'Kerala', 'Enneagram', 'india', 'TeczowaPolska', 'Philza', 'LOONA', 'TallGirls', 'wholesomeyuri', 'TSLALounge', 'GuildValkyrie', 'LGBTindia', 'CreatingAnUniverse', 'MotherMother', 'TwoXIndia', 'trans_irl', 'BigFamilyofGacha', 'entj', 'TwoootlesMemes', 'cheese_chat', 'ensemblestars', 'crosstradingroblox', 'chaoticacademia', 'FemboyOnlyFans', 'CitrusManga', 'Luciasshitposts', 'Midlifetrans', 'TFABLinePorn', 'shiirotokuurocringe', 'Art', 'ftmcirclejerk', 'crochet', 'exmuslim', 'alternativefashion', 'PERSoNA', 'FullmetalAlchemist', 'sandiegosocial', 'wendigoon', 'queersomnivexilology', 'Gaybies123ABC', 'Switzerland', 'softlewds', 'CuteTraps', 'assworship', 'StLouis', 'gaygeeks', 'QueerPeopleTwitter', 'findaname', 'Nonbeenary', 'albykitchin', 'chubbybois', 'eulalia', 'Diablo', 'Enbymemes', 'WritingPrompts', 'skatergirls', 'aspiekids', 'Transmedical', 'everything_roblox', 'RoyaleHighProductions', 'SmugIdeologyMan', 'bubblewriters', 'TheNebulAce', 'drawing', 'SecretSubreddit', 'CampingGear', 'Makeup', 'xxfitness', 'SquaredCircle', 'Random_Acts_Of_Amazon', 'creepshowart', 'indonesia', 'DemigodFiles', 'Cookierun', 'Reformed', 'GWASapphic', 'choruscory', 'popheadscirclejerk', 'MASFandom', 'animememes', 'MurderedByWords', 'lokean', 'Gunpla', 'heathenry', 'MemeAlleyway', 'gender', 'GaylorSwift', 'CompetitiveTFT', 'byulgbtq', 'RecRoom', 'hairfetish', 'OSDD', 'de', 'TheMentalIllnesses', 'TwennyWunPilots', 'audiodrama', 'HPfanfiction', 'forwardsfromgrandma', 'Ibispaintx', 'SelfHarmScars', 'averagedickproblems', 'SecularTarot', 'Do3jftinytowerDude', 'okbuddyretard', 'DietTea', 'circlejerk', 'Episode', 'cptsd_bipoc', 'geometrydash', 'DisabledBall', 'furrymemes', 'Daily_Gacha', 'femboyCompSci', '4tran', 'MobileLegendsGame', 'JustTransThangs', 'Witch', 'sydney', 'TooAfraidToAskLGBT', 'panicatthedisco', 'HentaiBirth', 'STOscreenshots', 'OnePunchMan', 'GamkaLifCrimg', 'toontownrewritten', 'walmart', 'infertility', 'greece', 'transgenre', 'LGBThonest', 'UTAustin', 'TeenagersCircleJerk', 'ralsei', 'introvert', 'RATS', 'TransTeensPassing', 'custommagic', 'Foamed', 'dontstarve', 'technicallythetruth', 'AskMenOver30', 'AnimeGacha', 'NSFWLW', 'truNB', 'PrequelMemes', 'noyoucantwatch', 'antifastonetoss', 'aegosexuals', 'JohnMayer', 'AskLE', 'monogamy', 'clittorturee', 'lemondemon', 'fundiesnarkiesnark', 'cats', 'im14andthisisdeep', 'SpeedOfLobsters', 'Cekrek', 'worldbuilding', 'furry_irl', 'salesengineers', 'okbuddybaka', 'Polysexual', 'StardewValley', 'SingleAndHappy', 'FIREyFemmes', 'latediagnosisadhd', 'VentiHentai', 'pacebi', 'rollerblading', 'mac', 'hoggit', 'LoveForLandlords', 'HowAreWeToday', 'Wizard101', 'Underrated_RH', 'Genderfae', 'FantasyToySex', 'Royalehighcoms', 'OnceUponATime', 'CrossdressersUK', 'averageredditor', 'BDSMpersonals', 'NonBinaryFurries', 'weezer', 'ChloeTheEgg', 'straightsasklgbt', 'HelluvaBoss', 'Teratophiliacs', 'ShitWehraboosSay', 'royalehighcomms', 'PolandballCommunity', 'MinecraftChampionship', 'slatestarcodex', 'gorillaz', 'traa_de', 'TheDragonPrince', 'Abrosexual', 'CampCamp', 'BlueGhost', 'ihadastroke', 'Bumble', 'Eragon', 'LGBTAustralia', 'RoyaleHigh_WFL', 'cross_tradingrblx', 'GachaFnafShipCringe', 'TrueTransChristians', 'jhu', 'RoyalHighCommunity', 'Genderstufff', 'bostoncollege', 'artc', 'OWConsole', 'SLAVONFIRE', 'MensHealthMatters', 'TNOmod', 'gsrm', 'traumacore', 'bookscirclejerk', 'FancyFollicles', 'MorpheusASMR', 'RoyaleHighHaloTrades', 'gaytaliaofficial', 'AskWomenOver30', 'arizonapolitics', 'KidnapKink', 'ShitAmericansSay', 'RoyalHighTradingHalos', 'replika', 'women', 'Trading_RoyaleHigh', 'billieeilish', 'dateademi', 'CrossTradingin_Roblox', 'deeeeeeeemmmmmmiii', 'BlueFoxDevelopment', 'RemoveOneThingEachDay', 'karens', 'PeriodPeople', 'SlasherTVSeries', 'FatPositiveWL', 'elliotrodgersmolbean', 'Philippines', 'Genshin_Impact_Leaks', 'thatveganteachersucks', 'pirakaplantmoment', 'LGBTQWattpad', 'ndwitches', 'mormon', 'Cross_Trading_Roblox_', 'TransTF2', 'AestheticColour_Gacha', 'LillyVinnilyCosplay', 'AcademyofEastShore', 'transylveon', 'cosplayteens', 'Nanny', 'AFAB_venting', 'Uranic', 'btd6', 'KingOfTheHill', 'cross_tradingrobloxx', 'SuddenlyLesbian', 'INW123sFans', 'giantbomb', 'weirdgamecrush', 'ftmyaoi', 'BisexualFantasy', 'Slycooper', 'cooltiktoks', 'Manitoba', 'AgreefulUpvote', '2meirl42meirl4meirl', 'alltimelow', 'moderatelygranolamoms', 'SocialJusticeInAction', 'Demigirlteens', 'LGBT_picrews', 'Crosstrading_cows', 'transitiongoals', 'IRLDemifemme', 'AnarchyChess', 'transfemmesnamedluna', 'WritingRH', 'LinuxYiff', 'Teachers', 'DebunkTransphobia', 'oldhagfashion', 'althomestuck', 'discord_irl', 'QueerCommunity', 'Charadefensesquad', 'GenderFluxx', 'AmongUs', 'FeminismUncensored', 'IndianTeenagers', 'CarolynHax', 'QueerWriting', 'GachaRp', 'ScamRefundsRoblox', 'IntensiveCare', 'wholesome_axolotls', 'suddenlysexoffender', 'steroids', 'SS13', 'deaftrans', 'IWantToBeHerHentai2', 'graysonsprojects', 'RobloxIslands_Dremsmp', 'HazbinHotel', 'holdup', 'CompetitionShooting', 'menslibIndia', 'ugly', 'FindMeADistro', 'vaxxhappened', 'Blacklight', 'petplaycommunity', 'Trimps', 'LeftWingLGBT', 'Judaism', 'heccra', 'stfuretard', 'PaganProles', 'ESFP', 'bettafish', 'socialwork', 'tranztalk', 'girlsthatareteens', 'enbyfootball', 'Professors', 'Osana', 'TomorrowByTogether', 'royalehightrading_art', 'RblxCrosstrade', 'F365Exiles', 'Neeeeopronouns', 'RoyaleHigh_Stuff', 'glasgow', 'RHdiamondtrading', 'unitedkingdom', 'ADHD_partners', 'queercodedvillians', 'horizon', 'kpop', 'nonbinarygonewild', 'socialskills', 'Chevy', 'PokemonSwordAndShield', 'FFXV', 'PenelopeScott', 'BeeSwarmSimulator', 'silentgenhumor', 'lgbtkzoo', 'TransFanfic', 'espresso', 'TransQueerHerbalCare', 'AnimeFunny', 'MinecraftMemes', 'unOrdinary', 'mspec_community', 'KindaBeckYT', 'tucutes', 'wondercottage', 'InfertilityBabies', 'FDSredpills', 'LibertariansOfAsia', 'Roblox_Scammers', 'CrappyDesign', 'Arthropod_kingmoment', 'YouthRights', 'Breath_of_the_Wild', 'whatisthisthing', 'ShiftingToTheAnimeMHA', 'BFDIBFDIAIDFBBFBTPOT', 'Rblx_Cross_Trading_', 'GachaTrans', 'queerporn', 'Justrolledintotheshop', 'Zillennials', 'paganism', 'DerScheisser', 'ps2', 'MassEffectMemes', 'LGBTQmafia', 'MouseReview', 'AdoptMehTading', 'computers', 'LoadedsCreamyMemes', 'petsimulater_x', 'Aegosexual', 'transtrans', 'Mental_illness_help', 'Psychosis', 'PetSimulatorX', 'Bonsai', 'serbia', 'ScaramoucheMains', 'xenogendercringe', 'Nudist_teens', 'oklahoma', 'tucker_carlson', 'TeenagersButBetter', 'PCMcj', '3Dprinting', 'gachaanimatorcringe', 'Kayaking', 'GODisGood7777777', 'PaintandSipYT', 'u_mazotori', 'TridentTrans', 'Cardinals', '12in12', 'WonderTrade', 'nintendo', 'edmproduction', 'CrohnsDisease', 'acturnips', 'Anki', 'shitpolandballsays', 'storyofseasons', 'MLPIOS', 'CrossTradingGiveaways', 'RobloxRHTrading', 'GachaWeebs', 'Supernatural', 'sagedoesonlyfans', 'Rblx_RoyaleHighTrades', 'Ineedhugs', 'u_doing_great_alone', 'Omniromantic', 'TheKikoDiner', 'SexAble', 'RPDR_UK', 'panonlyfans', 'nblnb', 'MinecraftPuzzles', 'Gachafanart', 'AkiAhnungslos', 'DanganronpaMEPs', 'TheBigWasteland', 'Aphantasia', 'CrazyHand', 'AdoptMeArtworks', 'KGATLW', 'TraaButNoCommies', 'ValidationStation', 'meth', 'Askasurvivor', 'classiccars', 'transaddicts', 'TrollGC', 'transpiring', 'CODWarzone', 'FtMteenagers', 'otherkringe', 'dyscalculia', 'MutualSupport', 'LGBDropTheT', 'extroverts', 'TransCA', 'MockTheAltRight', 'medical', 'TopsAndBottoms', 'rosehulman', 'techtheatre', 'PlanetCoaster', 'elderscrollsonline', 'healthcare', 'MAssociatedPress', 'gatech', 'SavedbyReginald', 'ftmmisogynyfetish', 'LGBTQperiods', 'TransDelaware', 'antitruscum', 'translego', 'Cultural_Marxism_irl', 'okbuddydengist', 'LGBDropTheTransphobes', 'TopSurgery', 'assassinscreed', 'CasualUK', 'libertarianmeme', 'truscumcirclejerk', '5Gays', 'OMORI', 'FaceOfAce', 'photography', 'fantasybball', 'transmasc_irl', 'teenagersactualone', 'CampingandHiking', 'TPUSACirclejerk', 'Queer_Club', 'QueerWomenOfColor', 'prolife', 'horror', 'rimjob_steve', 'u_Pastelliz', 'babykatastan', 'Allergies', 'RobloxTradingandTea', 'SHINee', 'RocketLeague', 'RoyalHighCommissions', 'lesleyism', 'CuteInnocentFun', 'ladyshavers', 'SouthernTransGang', 'Lehigh', 'books', 'translation', 'YasuoMains', 'RogerBlake', 'AshIsTrash', 'LeightonNight', 'MiloMurphysLaw', 'Bisexualfriends', 'fatpeoplestories', 'ImGoingToHellForThis', 'KimetsuNoYaiba', 'filtersweep', 'u_dont_eat_fresh', 'Alex', 'teensthataregay', 'yourmomshousepodcast', 'TransQualityGifs', 'NewSkaters', 'TransRavers', 'OliveMUA', 'loveafterlockup', 'ComedyNecrophilia', 'TransyPillowTalk', 'DreamWasTaken', 'hapas', 'witchcraft', '90DayFiance', 'ennnbyNSFW', 'LGBT_bandwagon', 'LGBTwitchcraft', 'CigarsOffTopic', 'uofm', 'BrandNewSentence', 'Mysticat', 'vce', 'YoekoKurahashi', 'AussieLibertarians', 'NiceVancouver', 'DnDFemaleAndNonBinary', 'SmashBrosUltimate', '90s', 'CrosszTrading', 'GachaClubAndroid', 'LeoSnowy', 'DenverCirclejerk', 'Inktober', 'AlcheMage_TavernInn', 'ethfinance', 'ultravioletYT', 'eformed', 'Cyberpunk', 'linkiscute', 'PetiteFashionAdvice', 'monstergirlsmemes', 'LonghornNation', 'GachaPals', 'edit_audios', 'LGBTQMentalHealth', 'fantasyrelationships', 'Taki', 'kpopfanfiction', 'trugender', 'WarframeRunway', 'JustARandomWoof', 'Creaturnomicon', 'TripleABattery', 'AtheistWitches', 'cross_tradiing', 'transfitness', 'Cutedogsreddit', 'RblxGiveawaysOfficial', 'inscryption', 'learnpolish', 'AO3', 'SocialismAndVeganism', 'BossfightUniverse', 'trueINTJ', 'softmaledom', 'gardening', 'Roblox_xCrosstrades', 'trans_centrism', 'CCW', 'thinkpad', 'totalwar', 'baseballcards', 'DelSol', '3dspiracy', 'intel', 'qnap', 'RotMG', 'Shadowverse', 'PokemonShuffle', 'spikes', 'teslamotors', 'keto', 'VaporVinyl', 'ElectricSkateboarding', 'CompTIA', 'Android', 'bicycling', 'geocaching', 'MTGLegacy', 'WeAreTheMusicMakers', 'Audi', 'indieheadscirclejerk', 'USContenders', 'oculus', 'PHBookClub', 'patientgamers', 'MaleFashionMarket', 'trumpet', 'Blazblue', 'GolfGTI', 'AmazfitBip', 'fantasyhockey', 'financialindependence', 'vaporents', 'ChineseLanguage', 'VerifiedSluts', 'Schizoid', 'Trombone', 'Slipknot', 'Mahoutokoro', 'childemains', 'ecr_eu', 'CarTalkUK', 'AustraliaSimLower', 'TeamGingerbread', 'opencarry', 'xxketo', 'MyrtleBeach', 'rawdenim', 'EDH', 'tucutecirclejerk', 'MotoUK', 'Tinder', 'Rateme', 'forgeofempires', 'bangtan', 'incremental_games', 'Gloryandgold', 'TeamTimesSquare', 'Kawasaki', 'aspergirls', 'Pokemongiveaway', 'BankBallExchange', 'htcone', 'dbfz', 'brexit', 'writing', 'windowsphone', 'Panera', 'AntiMennard', 'Summerrejects', 'unknownlgbt', 'asoiaf', 'XenogenderTryouts', 'abrogender', 'emmo']

monthly_results = dict()

def reset_reservoir_and_results():
    'returns an empty reservoir and per-sub results for the subs we are interested in'
    reservoir = dict()
    monthly_results = dict()
    for sub in subs:
        monthly_results.setdefault(sub, [])
        reservoir.setdefault(sub, {'N':0, 'K':0})
    return monthly_results, reservoir
    

def generate_k(subreddit: str, year:int, month:int, data=declarers):
    count = ((data['year'] == year) & (data['month'] == month) & (data['subreddit'] == subreddit)).sum()
    return count


def find_all_matches(text, regex):
    """Iterate through all regex matches in a text, yielding the span of each as tuple."""
    r = re.compile(regex)
    for match in r.finditer(text):
        yield (match.start(), match.end())


def inside_quote(text: str, span: tuple) -> bool:
    """
    Test if a span-marked match is inside a quoted line.
    Such lines in Reddit data begin with "&gt;".
    """
    end = span[1]
    relevant_text = text[:end]
    return True if re.search('&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a quote symbol and the match


def extract(args, comment: dict, regex: str, include_quoted: bool, outfile: TextIO):
    """
    Extract a comment text and all relevant metadata.
    If no regex is supplied, extract the whole comment leaving the span field blank.
    If a regex is supplied, extract each match separately with its span info.
    Discard regex matches found inside of a quoted line.
    """
    
    if args.return_all:
        comment = json.dumps(comment)
        _=outfile.write(comment+'\n')
    
    else:
        text = comment['body']
        user = comment['author']
        flairtext = comment['author_flair_text']
        subreddit = comment['subreddit']
        score = comment['score']
        date = comment['created_utc']
        
        # assemble a standard Reddit URL for older data
        url_base = "https://www.reddit.com/r/"+subreddit+"/comments/"
        oldschool_link = url_base + comment['link_id'].split("_")[1] + "//" + comment['id']

        # choose the newer "permalink" metadata instead if available
        permalink = "https://www.reddit.com" + comment['permalink'] if 'permalink' in comment.keys() else oldschool_link

        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if regex is None:
            span = None
            row = [text, span, subreddit, score, user, flairtext, date, permalink]
            csvwriter.writerow(row)
        else:
            for span in find_all_matches(text, regex):
                if not include_quoted and not inside_quote(text, span):
                    span = str(span)
                    row = [text, span, subreddit, score, user, flairtext, date, permalink]
                    csvwriter.writerow(row)


def filter(comment: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment breaks any of the filtering rules.
    This is for nuanced criteria so positives are kept for manual review.
    """
    if popularity_threshold is not None:
        if comment['score'] < popularity_threshold:
            return True, "score below defined threshold"
    
    text = comment['body']
    #if nlp(text)._.is_profane:
    #    return True, "offensive language"

    if "i'm a bot" in text.lower():
        return True, "non-human generated"
    
    return False, None


def relevant(comment: dict, args: argparse.Namespace, subs) -> bool:
    """
    Test if a Reddit comment is at all relevant to the search.
    This is for broad criteria so negatives are discarded.
    The filters are ordered by how unlikely they are to pass for efficiency.
    """

    if comment['subreddit'] not in subs: 
        return False
    
    filtered, second_return = filter(comment, args.popularity) if not args.dont_filter else False, None
    filtered = filtered[0]
    if filtered:
        return False

    if comment['author_flair_text'] is None:
        return True
    else:
        search = re.search(combined_negative_regex, comment['author_flair_text']) if args.case_sensitive else re.search(combined_negative_regex, comment['author_flair_text'], re.IGNORECASE)
        if search:
             return False
        else:
             return True
        #return True if not search else False


def write_csv_headers(outfile_path: str):
    """Write the headers for both the results file and the file for filtered-out hits."""
    with open(outfile_path, 'a', encoding='utf-8') as outf:
        headers = ['text', 'span', 'subreddit', 'score', 'user', 'flairtext', 'date', 'permalink']
        csvwriter = csv.writer(outf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)


def read_redditfile(file: str):
    """
    Iterate over the pushshift JSON lines, yielding them as Python dicts.
    Decompress iteratively if necessary.
    """
    # older files in the dataset are uncompressed while newer ones use zstd compression and have .xz, .bz2, or .zst endings
    if not file.endswith('.bz2') and not file.endswith('.xz') and not file.endswith('.zst'):
        with open(file, 'r', encoding='utf-8') as infile:
            for line in infile:
                l = json.loads(line)
                yield(l)
    else:
        for comment, some_int in read_lines_zst(file):
            yield json.loads(comment)

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		logging.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close() 


def within_timeframe(month: str, time_from: tuple, time_to: tuple) -> bool:
    """Test if a given month from the Pushshift Corpus is within the user's provided timeframe."""
    # a month's directory name has the format "RC YYYY-MM"
    month = re.sub('\.\w+$', '', month) # remove file ending
    y = int(month.split("_")[1].split("-")[0])
    m = int(month.split("-")[1])

    if time_from is not None:
        from_year, from_month = time_from[0], time_from[1]

        if y < from_year:
            return False
        if y == from_year and m < from_month:
            return False
    
    if time_to is not None:
        to_year, to_month= time_to[0], time_to[1]

        if y > to_year:
            return False
        if y == to_year and m > to_month:
            return False

    return True


def fetch_data_timeframe(input_dir: str) -> tuple:
    """
    Establish a timeframe based on all directories found in the input directory.
    Used when no timeframe was given by user.
    """
    months = [elem.replace("RC_", "") for elem in os.listdir(input_dir) if not elem.endswith(".txt")]
    months = [elem.replace("RS_", "") for elem in months]
    months = [elem.replace(".zst", "") for elem in months if elem.endswith('.zst')]

    months = sorted(months)
    months = [(int(elem.split("-")[0]), int(elem.split("-")[1])) for elem in months]
    return months[0], months[-1]


def establish_timeframe(time_from: tuple, time_to: tuple, input_dir: str) -> list:
    """Return all months of the data within a timeframe as list of directories."""
    months = [elem for elem in os.listdir(input_dir) if elem.startswith("RC") or elem.startswith("RS")] # all available months in the input directory

    return sorted([month for month in months if within_timeframe(month, time_from, time_to)], reverse=True)


def valid_date(string) -> tuple:
    """
    Check if a given date follows the required formatting and is valid.
    Returns a (year, month) tuple.
    Used as ArgParser type.
    """
    if re.search('^20[012]\d\-0?\d[012]?$', string):
        year, month = int(string.split("-")[0]), int(string.split("-")[1])
        if month > 12 or month < 1:
            msg = f"not a valid month: {month}"
            raise argparse.ArgumentTypeError(msg)
        else:
            return (year, month)
    else:
        msg = f"not a valid date: {string}"
        raise argparse.ArgumentTypeError(msg)


def dir_path(string) -> str:
    """
    Test if a given path exists on the machine.
    Used as ArgParser type.
    """
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)


def sample_float(num) -> float:
    try:
        num = float(num)
    except:
        raise TypeError(f"{num} is not a recognized number format.")
    
    if num > 1.0 or num < 0:
        raise TypeError("Sample size must be given as number between 0.0 and 1.0")
    
    return num


def comment_regex(string) -> str:
    """
    Some modifications for supplied regexes.
    Currently just to allow for quoted blocks to come at the beginning if the supplied regex asks for regex matches at the beginning of comments via ^
    """
    
    if os.path.isfile(string):
        regex = open(string, "r", encoding="utf-8").read()
    else:
        regex = string

    initial_regex_tester = "^((?:\(\?<[=!].*?\)))?(\^)" # to check if expression has ^ at beginning, while also allow for look-behind statements that can contain ^

    if re.search(initial_regex_tester, regex):
        flag = re.search(f'{initial_regex_tester}(.+$)', regex).group(1) # in case there is a flag of the type (?i) at the start
        flag = '' if flag is None else flag

        expr = re.search(f'{initial_regex_tester}(.+$)', regex).group(3)
        
        regex = flag+ '^' + r'(>.+\n\n)*' + expr
        logging.info(f"Regex changed to {regex}")

    return regex


def assemble_outfile_name(args: argparse.Namespace, month) -> str:
    """
    Assemble the outfile name out of the search parameters in human-readable and sanitized form.
    Full path is returned.
    """
    outfile_name = "comment_extraction_reservoir-sampled_based-on_pronoun-declarers_from-month_"

    # add timeframe info
    # this allows for the name to make sense with any or both of the timeframe bounds absent or present
    if month is not None:
        outfile_name += "_from_" + month
    else:
        if args.time_from is not None:
            outfile_name += "_from_" + str(args.time_from[0]) + '-' + str(args.time_from[1])
        if args.time_to is not None:
            outfile_name += "_up_to_" + str(args.time_to[0]) + '-' + str(args.time_to[1])
    # add time of search
    outfile_name += "_executed-at_" + datetime.now().strftime('%Y-%m-%d_at_%Hh-%Mm-%Ss')
    # specify the month of the reddit data
    outfile_name = outfile_name + "_" + month if month is not None else outfile_name
    # add file ending
    outfile_name += ".csv" if not args.return_all else ".jsonl"

    return outfile_name


def define_parser() -> argparse.ArgumentParser:
    """Define console argument parser."""
    parser = argparse.ArgumentParser(description="Keyword search comments from the Pushshift data dumps")

    # directories
    parser.add_argument('--input', '-I', type=dir_path, required=True,
                        help="The directory containing the input data, ie. the Pushshift data dumps.")
    parser.add_argument('--output', '-O', type=dir_path, required=False,
                        help="The directory where search results will be saved to.")
    
    # timeframe
    parser.add_argument('--time_from', '-F', type=valid_date, required=False,
                        help="The beginning of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no lower bound.")
    parser.add_argument('--time_to', '-T', type=valid_date, required=False,
                        help="The end of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no upper bound.")
    
    # search parameters
    parser.add_argument('--commentregex', '-CR', type=comment_regex, required=False,
                        help="The regex to search the comments with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--flairregex', '-FR', type=comment_regex, required=False,
                        help="The regex to search the comment flairs with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--case-sensitive', '-CS', action='store_true',
                        help="Makes search case-sensitive if any regex (comment or flair) was supplied.")
    parser.add_argument('--popularity', '-P', type=int, required=False,
                        help="Popularity threshold: Filters out comments with a score lower than the given value.")
    parser.add_argument('--toplevel', '-TL', action='store_true', required=False,
                        help="Only consider top-level comments, ie. comments not posted as a reply to another comment, but directly to a post.")
    parser.add_argument('--language', '-L', required=False,
                        help="Language to be used for spacy search.")
    
    # special
    parser.add_argument('--count', '-C', action='store_true',
                        help="Only counts the relevant comments per month and prints the statistic to console.")
    parser.add_argument('--include_quoted', action='store_true',
                        help="Include regex matches that are inside Reddit quotes (lines starting with >, often but not exclusively used to quote other Reddit users)")
    parser.add_argument('--sample', '-SMP', type=sample_float, required=False,
                        help="Retrieve a sample of results fitting the other parameters. Sample size is given as float between 0.0 and 1.0 where 1.0 returns 100% of results")
    parser.add_argument('--return_all', action='store_true', required=False,
                        help="Will return every search hit in its original and complete JSON form.")
    parser.add_argument('--dont_filter', action='store_true', required=False,
                        help="Skip any filtering.")

    return parser


def handle_args() -> argparse.Namespace:
    """Handle argument-related edge cases by throwing meaningful errors."""
    parser = define_parser()
    args = parser.parse_args()

    if args.output is None and not args.count:
        parser.error("Since you're not just counting, you need to supply an output directory.")

    # ensure that the timeframe makes sense (either the from-year is later than to-year, or the from-month is later than to-month in the same year)
    # only necessary if both endpoints are given
    if args.time_from is not None and args.time_to is not None:
        if args.time_from[0] > args.time_to[0] or (args.time_from[0] == args.time_to[0] and args.time_from[1] > args.time_to[1]):
            parser.error("argument --time_from is later than --time_to")
    # if no timeframe is given, all available months are searched
    elif args.time_from is None and args.time_to is None:
        logging.info("No timeframe supplied. Searching all months found in the input directory.")
        args.time_from, args.time_to = fetch_data_timeframe(args.input)

    return args


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)

def parse_month(month: str):
    "get year and month as integers from filename"
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = int(month.split("-")[0]) # get year string from the format 'RC_YYYY-MM.zst'
    month = int(month.split("-")[1]) # get month integer
    return month, year    

def get_data_file(path: str) -> str:
    """
    Find the correct file type of each month directory.
    Files can be plain, zst, xz, or bz2.
    Throw error if no usable file is present in directory.
    """
    for ending in ['', '.zst', '.xz', '.bz2']:
        if os.path.isfile(path+ending):
            return path+ending
    logging.warning("Month directory " + dir + " does not contain a valid data dump file.")
    exit()


def process_month(month, args, outfile):
    log_month(month)

    infile = args.input + "/" + month

    month, year = parse_month(month)
    
    monthly_results, reservoir = reset_reservoir_and_results()
    for sub in subs:
        reservoir[sub]['K'] = generate_k(sub, year, month, declarers)

    month_subs = [sub for sub in subs if reservoir[sub]['K'] > 0]

    for comment in read_redditfile(infile):
        if relevant(comment, args, month_subs):
            sub = comment['subreddit']
            k = reservoir[sub]['K']
            reservoir[sub]['N'] += 1
            n = reservoir[sub]['N']

            if len(monthly_results[sub]) < k:
                monthly_results[sub].append(comment)
            else:
                s = int(random.random() * n)
                if s < k:
                    monthly_results[sub][s] = comment

    with open(outfile, "a", encoding="utf-8") as outf:
        
        for sub in list(monthly_results.keys()):
            for comment in monthly_results[sub]:
                extract(args, comment, args.commentregex, args.include_quoted, outf)


def fetch_model(lang):
    if lang.lower() == "german" or lang.lower() == "deutsch":
        return 'de_dep_news_trf'
    else:
        logging.info("Only German spacy models are currently installed.")
        exit()


def main():
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')
    args = handle_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    logging.info(f"Searching from {timeframe[0]} to {timeframe[-1]}")

    # Writing the CSV headers
    for month in timeframe:
        outfile = assemble_outfile_name(args, month)
        outfile = os.path.join(args.output, outfile)
        if not args.return_all:
            write_csv_headers(outfile)
        process_month(month, args, outfile)
    


if __name__ == "__main__":
    main()