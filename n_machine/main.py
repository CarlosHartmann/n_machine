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
subs = ['linuxmasterrace', 'weddingplanning', 'KarlJacobs', 'goodanimemes', 'crosstradingroblox', 'FtMPorn', 'teslamotors', 'malingering', 'menslibIndia', 'totalwar', 'PuzzleAndDragons', 'demisexuality', 'TwoBestFriendsPlay', 'CCW', 'atwwdpodcast', 'FanFiction', 'Reformed', 'FTMfemininity', 'raisedbynarcissists', 'Guitar', 'F365Exiles', 'comingout', 'transnames', 'TheLastAirbender', 'RoyalHighTradingHub', 'SpoiledDragRace', 'music_survivor', 'LosAngeles', 'SexOnTheSpectrum', 'MTGLegacy', 'cars', 'LGBT_Muslims', 'PurplePillDebate', 'shadowpeople', 'Enneagram', 'RoyaleHigh_Giveaway', 'bi_irl', 'RobloxCrosstrading', '4tran', 'RoyaleHigh_Refunds', 'FTMMen', 'Animemes', 'LibJerk', 'selfharmteens', 'AmongUs', 'PlusSize', 'stilltrying', 'Jreg', 'ShitLiberalsSay', 'muacjdiscussion', 'ACTrade', 'FTM_SELFIES', 'popheadscirclejerk', 'GWASapphic', 'actual_detrans', 'leagueoflegends', 'HogwartsGhosts', 'GenderCynical', 'CanadaPolitics', 't4t', 'birthcontrol', 'asiantransgender', 'infertility', 'masstagger', 'QueerStem', 'EDanonymemes', 'Stims', 'popheads', 'FFBraveExvius', 'dreamgender', 'TransIreland', 'mypartneristrans', 'transteens', 'IWantOut', 'toxicparents', 'Reduction', 'PSO2', 'LesbianActually', 'Therian', 'churning', 'UCSC', 'indonesia', 'ftmspunished', 'autism', 'GayChristians', 'automationgame', 'SwordPansexual', 'NotHowGirlsWork', 'cross_trading_roblocs', 'illnessfakersgonewild', 'OkCupid', 'BreadTube', 'foreverbox', 'UUreddit', 'FreedTheNips', 'snapcube', 'pregnant', 'plural', 'deadbydaylight', 'menstrualcups', 'femby', 'FantasyToy', 'TheNebulAce', 'truechildfree', 'transhumanism', 'RobloxIslands_Dremsmp', 'creepshowart', 'pcmasterrace', 'badunitedkingdom', 'osugame', 'TransSpace', 'Teachers', 'Negareddit', 'PostTransitionTrans', 'SRSsucks', 'ccna', 'camaro', 'WritingPrompts', 'Undertale', 'baseballcards', 'Cumtown', 'socdemnetwork', 'RomanceClub', 'EatingIntuitively', 'theroyalrealms', 'tucker_carlson', 'PansexualTeens', 'RoyaleHighTrading__', 'INTP', 'FatPositiveWL', 'DIYemo', 'traadustCrusaders', 'Rollerskating', 'transvoice', 'sto', 'ControversialOpinions', 'MHOCPress', 'phlgbt', 'ProtectAndServe', 'transnord', 'animecirclejerk', 'CPTSDmemes', 'europeanunion', 'FTMOver30', 'ChloeTheEgg', 'europe', 'TheSilphRoad', 'TransDIY', 'GatekeepingYuri', 'gayjews', 'keto', 'ddlg', 'PrincessesOfPower', 'TSLALounge', 'papermario', 'transadorable', 'ScaramoucheMains', 'edmproduction', 'beholdthemasterrace', 'fantasyrelationships', 'SmugIdeologyMan', 'RotMG', 'transgenderteens', 'DotA2', 'Roblox_Scammers', 'berkeley', 'SquaredCircle', 'CPTSD', 'Choices', 'prolife', 'medicine', 'lesbianteens', 'starcitizen', 'forhonor', 'prochoice', 'RustyQuill', 'heathenry', 'pokemongo', 'butchlesbians', 'serbia', 'Toonami', 'Periods', 'pirakaplantmoment', 'AprilBumpers2018', 'CoronavirusCirclejerk', 'transfurs', 'Trading_RH', 'shiftingrealities', 'RoleReversal', 'questioning', 'askGSM', 'Cubers', 'TranslationStudies', 'stevenuniverse', 'conlangs', 'TeraOnline', 'Deltarune', 'aromanticasexual', 'Wizard101', 'TumblrPls', 'fullegoism', 'bigboobproblems', 'india', 'MakeupAddiction', 'BPDmemes', 'translator', 'RadicalChristianity', 'uofmn', 'LGBTQCORNER', 'AdoptMehTading', 'salmacian', 'wedding', 'DID', 'ENLIGHTENEDCENTRISM', 'Warthunder', 'engineering', 'SwitchHacks', 'gaybroscirclejerk', 'transgenderau', 'ElectricSkateboarding', 'Audi', 'TransVent', 'oldhagfashion', 'FundieSnarkUncensored', 'GenderCriticalTheory', 'GradSchool', 'TransTF2', 'antiwork', 'transontario', 'MTFsPunished', 'always_lsg', 'CountryhumansCringe', 'MHEIAEd_BookStudy', 'IWantToBeHerHentai2', 'EuropeanFederalists', 'animememes', 'TransMasc', 'ActualPublicFreakouts', 'GachaClubCringe', 'rutgers', 'celestegame', 'BattleAxeBisexualVibe', 'Doom', 'StarVStheForcesofEvil', 'latterdaysaints', 'uwaterloo', 'GirlGamers', 'RoyalHighCommunity', 'menstruation', 'Proust', 'TagProIRL', 'Enough_Sanders_Spam', 'geometrydash', 'MouseReview', 'intersex', 'hairfetish', 'otomegames', 'neopets', 'LGBT_picrews', 'RocketLeague', 'oculus', 'fakebaseball', 'AskStudents_Public', 'harrypotter', 'ShitRedditSays', 'Do3jftinytowerDude', 'ffxiv', 'asklatinamerica', 'grandorder', 'xenogendercringe', 'VaporVinyl', 'amateurradio', 'WingsOfFire', 'modular', 'HentaiBirth', 'Warframe', 'genderfluid_irl', 'RobloxCommissions', 'FtMpassing', 'tickling', 'CrossTradingGiveaways', 'MakeupLounge', 'Episode', '3dspiracy', 'onejoke', 'fatlogic', 'CompTIA', 'GachaRp', 'RemoveOneThingEachDay', 'AskAnAmerican', 'vancouver', 'Rblx_RoyaleHighTrades', 'Lal_Salaam', 'GaymersGoneMild', 'hotleft', 'AcademyofEastShore', 'Drama', 'MultipleSclerosis', 'UnderworldOffice', 'exmuslim', '911dispatchers', 'germantrans', 'MHOCStormont', 'TheQueerKiwi', 'SandersSides', 'CalPoly', 'mbtimemes', 'entp', 'PMDD', 'Schizoid', 'Kirby', 'aoe2', 'nudism', 'CountryHumans', 'BeautyGuruChatter', 'AssCredit', 'JellesMarbleRuns', 'rit', 'ToiletPaperUSA', 'FTMFitness', 'transeducate', 'OSDD', 'GachaUnity', 'ForzaHorizon', 'SlasherTVSeries', 'Parahumans', 'speculum', 'SwitchHaxing', 'LegalAdviceUK', 'TAZCirclejerk', 'volleyball', 'SavedbyReginald', 'MillerPlanetside', 'MASFandom', 'BabyBumps', 'stevens', 'BDSMcommunity', 'Nanny', 'EnbyFashionAdvice', 'gaytransguys', 'TransBuyAndSell', 'arknights', 'beautytalkph', 'tokipona', 'MasculineOfCenter', 'skinnygossip', 'insaneparents', 'analog', 'SASSWitches', 'TransTeensMeet', 'lgbtchoicesfans', 'sterilization', 'SBU', 'TokyoAfterschool', 'rupaulsdragrace', 'DreamWasTaken2', 'MobileLegendsGame', 'BisexualHumans', 'Grimdank', 'TrueSTL', 'PupPlay', 'Mental_illness_help', 'destiny2', 'EdAnonymousAdults', 'CapitalismVSocialism', 'TryingForABaby', 'tranimemes', 'homestuck', 'selfharm_memes', 'TERFisafetish', 'LoveNikki', 'IBO', 'RPI', 'Panera', 'projectcar', 'Autos', 'socialism', 'mylittlepony', 'RHPhotoshoots', 'loseit', 'Android', 'mitski', 'discworld', 'LilyIsTrans', 'u_darlinqvera', 'CrossTradingin_Roblox', 'videography', 'rpdrcirclejerk', 'BlueGhost', 'nus', 'CompetitiveTFT', 'Rainbow6', 'Embroidery', 'petsimulater_x', 'EnbyandChill', 'Beto2020', 'Lovelink', 'USContenders', 'ffacj', 'PokemonSwordAndShield', 'FindMeADistro', 'pebble', 'EternalCardGame', 'elliotrodgersmolbean', 'NoRules', 'IndianTeenagers', 'cptsd_bipoc', 'gaybros', 'fo4', 'LateStageCapitalism', 'Orientedaroace', 'HalosTheRoleplay', 'SubredditDrama', 'ADMU', 'greenday', 'libertarianmeme', 'VaushV', 'AskElectronics', 'LeoSnowy', 'knives', 'EliteDangerous', 'bubblewriters', 'Lewd', 'pokemon', 'truNB', 'RoyaleHigh_Roblox', 'APStudents', 'touhou', 'FeminismUncensored', 'TopMindsOfReddit', 'transprogrammer', 'Tulpas', 'Outlander', 'electronic_cigarette', 'Christianity', 'CarTalkUK', 'TransAdoption', 'badlinguistics', 'splatoon', 'ArtCrit', 'AskDID', 'GamerGhazi', 'windowsphone', 'spikes', 'GaylorSwift', 'Battlefield', 'AntiGachasCringe', 'queensofleague', 'roblox', 'feedthebeast', 'Gunpla', 'Cumrades', 'RWBY', 'lgbtkzoo', 'AntifascistsofReddit', 'AnimalCrossing', 'IZ_ILUV', 'CHAZRevolution', 'circlejerk', 'KCRoyals', 'LesbianGamers', 'cscareerquestions', 'college', 'xxketo', 'Kerala', 'AttackOnRetards', 'TalesFromYourServer', 'twilight', 'DanganronpaCringe', 'pokemonshowdown', 'ADHD_partners', 'WastedGachaTalent', 'DIDmemes', 'FULLCOMMUNISM', 'Rblx_Cross_Trading_', 'Drexel', 'financialindependence', 'NoRulesCalgary', 'lowspecgamer', 'Unexpected', 'curlyhair', 'cataclysmdda', 'TumblrCirclejerk', 'notlikeothergirls', 'SequelMemes', 'Megaman', 'demiromantic', 'WholesomeTeenBoys', 'otherkin', 'aarrrooooooaaacceeeee', 'Bonsai', 'SuddenlyTrans', 'NorthCarolina', 'TrollCoping', 'fakedisordercringe', 'weddingyuri', 'ImGoingToHellForThis', 'Overwatch', 'TheArcana', 'TTC30', 'MysteryDungeon', 'SmolBeanSnark', 'de', 'BPD', 'worldbuilding', 'DBDGoneGay', 'ukpolitics', 'DnDFemaleAndNonBinary', 'Persona5', 'CoiningTerms', 'Fixxit', 'TwennyWunPilots', 'twittermoment', 'forcedbreeding', 'intel', 'Shadowverse', 'crochet', 'thegooddoctor', 'straykids', 'Markiplier', 'trumpet', 'Transpies', 'dankmemes', 'DIDart', 'knifeclub', 'rainbowcoven', 'lgbtqia_poc', 'BisexualMen', 'BeeSwarmSimulator', 'u_doing_great_alone', 'massage', 'BGCCircleJerk', 'transbookclub', 'WPI', 'Greysexuality', 'subnautica', 'goodyearwelt', 'Tokophobia', 'duolingo', 'NoFeeAC', 'Insurance', 'greece', 'InfertilityBabies', 'Switzerland', 'LibertariansOfAsia', 'BugFables', 'TrueTransChristians', 'CarletonU', 'RPGcreation', 'mixedrace', 'Breath_of_the_Wild', 'HPfanfiction', 'RobloxRHTrading', 'cottagecore', 'GachaTrans', 'NintendoSwitch', 'Acceleracers', 'TumblrAtRest', 'ABoringDystopia', 'ABDL', 'enbynsfw', 'xboxone', 'pokemontrades', 'CreepyWikipedia', 'Genderfae', 'snails', 'LateStageGenderBinary', 'Mtf_irl', 'ScamRefundsRoblox', 'softlewds', 'teenboys', 'TeamGingerbread', 'slashdiablo', 'neocentrism', 'redditmoment', 'LGBTARMY', 'Cekrek', 'entj', 'DemiFemme', 'teenagersgaming', 'AskAcademia', 'blogsnark', 'halo', 'AskGirls', 'WLWs_and_Wyrms', 'fuckeatingdisorders', 'PrideFlags', 'communism101', 'filtersweep', 'AnimeFunny', 'Midlifetrans', 'pacebi', 'multilingualparenting', 'TalesFromTheCustomer', 'adhdmeme', 'Libertarian', 'txstate', 'AnarchismZ', 'PositiveArtReviews', 'headphones', 'AceTeens', 'short', 'GothStyle', 'vegan', 'tankiejerk', 'MtFteens', 'italianlearning', 'WroteAThing', 'emotionalabuse', 'NSFWLW', 'Daily_Gacha', 'NoStupidQuestions', 'GH5', 'OkBuddyCatgirl', 'muacirclejerk', 'Transmedical', 'asktransmen', 'playstation', 'queerception', 'FoxStevenson', 'SonicTheHedgehog', 'polandball', 'asoiaf', 'RHcosplayCommunity', 'Pride_and_Positivity', 'PHBookClub', 'IncelTear', 'enfj', 'qnap', 'RblxDrawing', 'DemiGirl', 'RPGdesign', 'TheTCC', 'RadicallyOpenDBT', 'TransClones', 'TwoootlesMemes', 'Lgbtforestkids', 'eformed', 'LGBTindia', 'MaleFashionMarket', 'SatisfactoryGame', 'astrology', 'RoyalHighTradingHalos', 'Purdue', 'Grej', 'wholesomeyuri', 'vaginismus', 'Futurology', 'PERSoNA', 'SocialistRA', 'RivalsOfAether', 'queerfashions', 'Ibispaintx', 'mildlyinfuriating', 'SelfHarmScars', 'ShitAmericansSay', 'storyofseasons', 'THE_PACK', 'AmazfitBip', 'BMW', 'bettafish', 'TNOmod', 'Megaten', 'UrinatingTree', 'leftistvexillology', 'PleaseCallMeRedScarf', 'Genshin_Impact_Leaks', 'billycobb', 'UCDavis', 'nonbinarylesbians', 'Archery', 'LillyVinnilyCosplay', 'SimDemocracy', 'giantbomb', 'u_Angie-The-Rat', 'polyfamilies', 'gigantic', 'OpenUniversity', 'auntienetwork', 'QueerWriting', 'technicallythetruth', 'CarolynHax', 'Invisaforce', 'JudgeMyAccent', 'averagedickproblems', 'VentiHentai', 'QueerClimbers', 'transtwincities', 'PlasticGuitarSurgeon', 'BB30', 'NonBinaryGamers', 'pyrocynical', 'socialwork', 'Cringetopia', 'WritingRH', 'DBZDokkanBattle', 'CrankGameplays', 'fantasybball', 'notliketheothergirls', 'AccidentalPrideFlags', 'pawsomeanimals', 'CROSSTRADINGrh', 'unpopularkpopopinions', 'tarot', 'PetiteFashionAdvice', 'wholesomejojo', 'InclusiveMenstruation', 'clittorturee', 'biromantic', 'infp', 'ftmtimelines', 'harrystyles', 'MakeNewFriendsHere', 'BlakedAlaska', 'StarKid', 'plural_irl', 'ralsei', 'TwoXIndia', 'arttocope', 'BlakeTheFemboy', 'MonsterHunterWorld', 'Aritzia', 'PrequelMemes', 'FemboyOnlyFans', 'KailahsCringe', 'traabutyescommies', 'shittyMBTI', 'melanatedandmodified', 'F1NN5TER', 'iastate', 'MurderedByWords', 'solotravel', 'youngadults', 'transfemmesnamedluna', 'computers', 'omni_teens', 'chaoticacademia', 'TooAfraidToAskLGBT', 'Pokemongiveaway', 'HalfLife', 'STOscreenshots', 'Eragon', 'writerchat', 'cross_tradiing', 'mentalhealth', 'TransandroSupport', 'exchristian', 'acturnips', 'HelluvaBoss', 'SNSD', 'MTU', 'MotoUK', 'CampCamp', 'UIUC', 'Rateme', 'ExPentecostal', 'namenerds', 'vegancirclejerk', 'EDH', 'u_dont_eat_fresh', 'panicatthedisco', 'ACNHTrade', 'cancer', 'furry_irl', 'radicalqueers', 'Philippines', 'DarkFics', 'BDSMpersonals', 'transbooks', 'aviation', 'DuelLinks', 'trans_fat', 'Genderstufff', 'okbuddybaka', 'pagan', 'ApexUncovered', 'Puberty', 'RecRoom', 'offmychest', 'trixic', 'WineEP', 'rawdenim', 'gachacoolers', 'Mahoutokoro', 'TransgenderIncels', 'geocaching', 'DDLC', 'UCI', 'MLPIOS', 'RobloxRoyaleHigh_', 'forwardsfromgrandma', 'poledancing', 'byulgbtq', 'nonbinaryUK', 'ems', 'thatveganteachersucks', 'poetry_critics', 'incremental_games', 'bemorechill', 'unitedkingdom', 'BankBallExchange', 'Skookum', 'NiceVancouver', 'chubbybois', 'Zillennials', 'TaylorSwift', 'MakeMeSuffer', 'tall', 'lokean', 'cross_tradingrobloxx', 'ModelResources', 'OWConsole', 'universityofauckland', 'aegosexuals', 'vaporents', 'Twitch', 'CrimsonNecklace', 'askhotels', 'knitting', 'German', 'steroids', 'jhu', 'SocialistGaming', 'YasuoMains', 'Hasan_Piker', 'althomestuck', 'sagedoesonlyfans', 'EdgingTalk', 'alltheleft', 'SexPositiveNofap', 'transgenre', 'DebunkTransphobia', 'bardmains', 'ObeyMeNSFW', 'Cardinals', 'DualGender', 'NBA2k', 'JohnMayer', 'indieheadscirclejerk', 'TheGoodPlace', 'AustraliaSimLower', 'DemiboyCulture', 'karens', 'Eurosceptics', 'facepalm', 'XenogenderTryouts', 'ABBACircleJerk', 'transmasculine', 'summonerschool', 'JustTransThangs', 'espresso', 'AskTeenGirls', 'AskEnbies', 'centralmich', 'shitfascistssay', 'DerScheisser', 'toddlers', 'ChineseLanguage', 'talesfromtechsupport', 'GachaEdits', 'sse', 'AskWomenOver30', 'martialarts', 'magicthecirclejerking', 'arizonapolitics', 'deeeeeeeemmmmmmiii', '8ValuesMemes', 'Paramore', 'tucutes', '0sanitymemes', 'MechanicalKeyboards', 'Fallout', 'royalehightrading_art', 'BrujeriaEnglish', 'GenderFluxx', 'CrosszTrading', 'BigFamilyofGacha', 'QueerPeopleTwitter', 'LGBTQ_Safe_Space', 'XboxSeriesX', 'OKCupidLGBT', 'AmIASexyQueer', 'techtheatre', 'FullmetalAlchemist', 'TalesFromLGBT', 'AreTheTransOkay', 'fantasyhockey', 'noyoucantwatch', 'FDS_is_Transphobic', 'RedditForGrownups', 'language_exchange', 'ensemblestars', 'crosstrading_rblxxx', 'TFABLinePorn', 'transalute', 'DragonAgeCoOp', 'royalehighroleplay', 'WTT_graduates', 'IBEW', 'Vaping', 'vaxxhappened', 'SingleAndHappy', 'Connieverse', 'panromantic', 'sydney', 'detrans', 'TransPunished', 'heccra', 'ShrugLifeSyndicate', 'swtor', 'French', 'flightsim', 'GlobalTalk', 'RoyalHighCommissions', 'ScienceTeachers', 'OKState', 'Logic_Studio', 'audiodrama', 'Alex', 'antitruscum', 'FundieSnark', 'Diablo', 'ftmmisogynyfetish', 'Gaybies123ABC', 'sandiegosocial', 'albykitchin', 'toontownrewritten', 'panonlyfans', 'MemeAlleyway', 'rollercoasters', 'ChurchOfWaterLaw', 'tucutecirclejerk', 'titanfall', 'MaladaptiveDreaming', 'Abrosexual', 'MyChemicalRomance', 'GachaPals', 'Luciasshitposts', 'skyrimrequiem', 'LGBTQmafia', 'trans_irl', 'disenchantment', 'lunarcraft', 'cheese_chat', 'Fishing', 'yaoi', 'civ', 'Charadefensesquad', 'ESFP', 'MakeupRehab', 'TheMentalIllnesses', 'EnoughCommieSpam', 'LGBTQWattpad', 'suddenlybi', 'custommagic', 'arrow', 'sjwhate', 'trugender', 'kitseckshouse', 'Teratophiliacs', 'im14andthisisdeep', 'KingOfTheHill', 'gamegrumps', 'CompetitionShooting', 'ethfinance', 'MinecraftMemes', 'terfsplaining', 'cruze', 'bangtan', 'ainbowregion', 'Royalehighcoms', 'feedthetraa', 'Nonbeenary', 'lucifer', 'RomanceBooks', 'LGBTQperiods', 'livesound', 'alltimelow', 'LGBThonest', 'OnePunchMan', 'nerdfighters', 'GachaUwU', 'fea', 'petplay', 'AskAsexual', 'thomastheplankengine', 'otherkringe', 'weirdgamecrush', 'JustARandomWoof', 'AliceOseman', 'FantasyToySex', 'dpdr', 'Bisexualfriends', 'DreamWasTaken', 'infj', 'TheDragonPrince', 'writing', 'CSULB', 'GachaFnafShipCringe', 'DunderMifflin', '3Dprinting', 'choruscory', 'astrologymemes', 'bookscirclejerk', 'wholesome_axolotls', 'Trimps', 'BisexualFantasy', 'Omniromantic', 'Ineedhugs', 'TransMakeupHeaven', 'nonbinarygonewild', 'monogamy', 'FDSredpills', 'CrohnsDisease', 'RoyaleHigh_WFL', 'Cross_Trading_Roblox_', 'Crosstrading_cows', 'RATS', 'Blacklight', 'dndnext', 'medical', 'SexyDimorphism', 'PiercedCock', 'healthcare', 'Temple', 'VeganForCircleJerkers', 'OliveMUA', 'LateStageImperialism', 'StLouis', 'overclocking', 'TwoXSupport', 'starbound', 'Cookierun', 'lfg', 'Blazblue', 'AtheistWitches', 'Gachatardhaven', 'WVU', 'HollowKnight', 'AroAceAgender', 'fursuitsex', 'Bigender_irl', 'Bumble', 'MinecraftChampionship', 'AshIsTrash', 'slatestarcodex', 'subFoxo7002', 'Polandballart', 'loveafterlockup', 'babykatastan', 'LOONA', 'bigonewild', 'ComedyNecrophilia', 'Nudist_teens', 'Manitoba', '4ChanMeta', 'PokkenGame', 'FTMPornNoHole', 'traaaaaaarrrrrrrro', 'AnarchyChess', 'ShiftingToTheAnimeMHA', 'childemains', 'gaytaliaofficial', 'Unitale', 'xkcd', 'guitarcirclejerk', 'GuildValkyrie', 'CigarsOffTopic', 'Kindred', '5Gays', 'MensHealthMatters', 'alternativefashion', 'gaygeeks', 'RPDR_UK', 'queersomnivexilology', 'okbuddyableist', 'TeamTimesSquare', 'CuteTraps', 'transpoc', 'Vanderbilt', 'meth', 'micronations', 'UTAustin', 'PenelopeScott', '6thForm', 'lemondemon', 'AdoptMeArtworks', 'menstruationstation', 'germany', 'houkai3rd', 'iacceptyou', 'Anxiety', 'lgbthistory', 'AskAromantics', 'asexualdating', 'QueerSimmers', 'AskLE', 'periods_for_all', 'Jewish', 'Aegosexual', 'gsrm', 'CollegeRant', 'okbuddyretard', 'TransTeensPassing', 'diyelectronics', 'cfs', 'btd6', 'introvert', 'SecularTarot', 'RHdiamondtrading', 'BuddyCrossing', 'dontstarve', 'RoyaleHigh_Artt', 'whatsthisbug', 'moderatelygranolamoms', 'lostgeneration', 'Arthropod_kingmoment', 'PeriodPeople', 'BorderlinePDisorder', 'Intactivism', 'blackladies', 'surrealNBmemes', 'anarchocommunism', 'Polysexual', 'transfitness', 'Rolla', 'ladyshavers', 'Art', 'u_Pastelliz', 'furrymemes', 'IronFrontUSA', 'Cutedogsreddit', 'patientgamers', 'transmasc_irl', 'GachaWeebs', 'ugly', 'queerwitches', 'traa_de', 'ftmcirclejerk', 'TransRavers', 'walmart', 'Drag', 'VirginiaTech', 'Wattpad', 'artc', 'Speciesdysphoria', 'PaintandSipYT', 'Collegeauditions', 'shittylgbt', 'LGBTeensDate', 'femboyCompSci', 'traversecity', 'Justrolledintotheshop', 'cross_trading_robIx', 'Trombone', 'IRLDemifemme', 'CODWarzone', 'LinuxYiff', 'DietTea', 'UPenn', 'PopHeadsGossip', 'eddit', 'GenZanarchist', 'YouthRights', 'UofT', 'GuildWars', 'DQBuilders', 'CasualUK', '2007scape', 'Uranic', 'enbyfootball', 'teensthataregay', 'Scriptcraftguild', 'ihadastroke', 'AntiLGBTQ', 'Infiniteaxesmemes', 'wendigoon', 'PaganProles', 'ShitWehraboosSay', 'oddlysatisfying', 'ArosShow', 'weezer', 'translego', 'SkincareAddiction', 'CrossdressersUK', 'Tinder', 'TeczowaPolska', 'OMORI', 'bicycling', 'latediagnosisadhd', 'pcgaming', 'MinecraftPuzzles', 'MassEffectMemes', 'BeachCity', 'traaaaaaarrrrrrrroace', 'xxsurfing', 'wormswithlegs', 'AskMenOver30', 'dyscalculia', 'QContent', 'YoekoKurahashi', 'mysticmessenger', 'rosehulman', 'LGBTwitchcraft', 'MaleEatingDisorders', 'ennnbyNSFW', 'SexWorkersOnly', 'transED', 'queergonewildstories', 'MorpheusASMR', 'TransFanfic', 'polyglot', 'TransQueerHerbalCare', 'Cameras', 'transaddicts', 'unOrdinary', 'mspec_community', 'transpiring', 'KGATLW', 'Aphantasia', 'elderscrollsonline', 'TransDelaware', 'nblnb', 'okbuddydengist', 'Supernatural', 'teenagersactualone', 'rimjob_steve', 'Allergies', 'books', 'TridentTrans', 'Kayaking', 'oklahoma', '90s', 'LonghornNation', 'CrappyDesign', 'edit_audios', 'Creaturnomicon', 'wondercottage', 'Lehigh', 'Marxism', 'ClubChubPositivity', 'PokemonTCG', 'TraaButOnlyBees', 'BlueFoxDevelopment', 'DebateAnarchism', 'ndwitches', 'transylveon', 'FreeBeauty', 'the1975', 'PCOS', 'HeadphoneAdvice', 'NonBinaryFurries', 'schooloftrans', 'Lovestruck', 'BDSMGW', 'SnepYiff', 'LGBTQ_AnimalCrossing', 'Trading_RoyaleHigh', 'Philza', 'Anglicanism', 'fragileancaps', 'royalehighcomms', 'HowAreWeToday', 'AGPAAPmemes', 'Crosstradesxroblox', 'LGBTAustralia', 'asexualteens', 'TransRoachCult', 'yurimemes', 'bostoncollege', 'FIREyFemmes', 'Evasexual', 'salesengineers', 'KidnapKink', 'AnarchoWave', 'obeyme', 'dreamnotfound', 'Neeeeopronouns', 'girlsthatareteens', 'TransCommunity', 'eulalia', 'lesbianfashionadvice', '2meirl42meirl4meirl', 'Judaism', 'ThomasDoesMath', 'shiirotokuurocringe', 'CreatingAnUniverse', 'transgeneral', 'YUROP', 'TallGirls', 'DebateCommunism', 'SocialJusticeInAction', 'tsuyonpu', 'SF4', 'unixporn', 'GamkaLifCrimg', 'badreligion', 'Vaporwave', 'SS13', 'TransQualityGifs', 'LGBTQquestions', 'skatergirls', 'gatech', 'pan_irl', 'MAssociatedPress', 'NewSkaters', 'findaname', 'aspiekids', 'u_rh_bunny', 'hbomberguy', 'CitrusManga', 'jregHot', 'LollyBopSong', 'LeightonNight', 'LGBDropTheTransphobes', 'RogerBlake', 'assassinscreed', 'lesleyism', 'MotherMother', 'heart_stopper', 'glasgow', 'truscumcirclejerk', 'horror', 'CornGuy', 'SHINee', 'AngelSanctuary', 'brisbane', 'ModernMagic', 'MrRobot', 'BestOfOutrageCulture', 'AdptmeAndRhTrading', 'VerifiedSluts', 'jeanshate', 'podcasting', 'MechanicalEngineering', 'peopleofwalmart', 'siyoungiyo', 'Gloryandgold', 'aspergirls', 'htcone', 'dbfz', 'circlebroke2', 'Summerrejects', 'SanAntonioLGBTQIA', 'justlegbeardthings', 'Nexus', 'witchcraft', 'PragerUrine', 'femalefashion', 'LGBT_bandwagon', 'LeftistLGBTMemes', 'AussieLibertarians', 'Cyberpunk', 'monstergirlsmemes', 'speedofliberals', 'LGBTQMentalHealth', 'RoyaleHighProductions', 'WarframeRunway', 'PokemonROMhacks', 'TripleABattery', 'Slovakia', 'inscryption', 'Roblox_xCrosstrades', 'shittyfalloutlore', 'SkyrimPorn', 'PlanetCoaster', 'emmo', 'replika', 'cats', 'AestheticColour_Gacha', '12in12', 'petplaycommunity', 'LoveForLandlords', 'AkiAhnungslos', 'PolandballCommunity', 'HazbinHotel', 'silentgenhumor', 'KindaBeckYT', 'paganism', 'billieeilish', 'Chevy', 'classiccars', 'TrollGC', 'fundiesnarkiesnark', 'cosplayteens', 'queerporn', 'CampingGear', 'gachaanimatorcringe', 'IntensiveCare', 'Slycooper', 'Psychosis', 'BFDIBFDIAIDFBBFBTPOT', 'FixedGearBicycle', 'PCMcj', 'discord_irl', 'transitiongoals', 'StardewValley', 'SuddenlyLesbian', 'ps2', 'ftmyaoi', 'WeAreTheMusicMakers', 'lgbtcirclejerk', 'INW123sFans', 'AFAB_venting', 'cooltiktoks', 'queerean', 'MHOCStrangersBar', 'BossfightUniverse', 'women', 'PetSimulatorX', 'midsize', 'satanism', 'gqfitness', 'RblxGiveawaysOfficial', 'learnpolish', 'AO3', 'SocialismAndVeganism', 'trueINTJ', 'sciencesays1gender', 'softmaledom', 'gardening', 'trans_centrism', 'waifuism', 'transtrans', 'highschoolfootball', 'dateademi', 'LoadedsCreamyMemes', 'LethalLeague', 'spongebob', 'drawing', 'AgreefulUpvote', 'horizon', 'forgeofempires', 'tranztalk', 'FFXV', 'Professors', 'Kawasaki', 'socialskills', 'rblxtrading_', 'Osana', 'kpop', 'brexit', 'supergirlTV', 'Dermatillomania', 'queercodedvillians', 'WeAreSorry', 'AntiMennard', 'TomorrowByTogether', 'unknownlgbt', 'queermakeup', 'RblxCrosstrade', 'abrogender', 'stfuretard', 'SIFallstars', 'Dragula', 'LeftWingLGBT', 'whatisthisthing', 'Demigirlteens', 'QueerCommunity', 'proudlibtard', 'suddenlysexoffender', 'deaftrans', 'nb_mp', 'BikiniBottomUnion', 'graysonsprojects', 'Slipknot', 'holdup', 'SailorMoonDrops', 'ecr_eu', 'TowerOfBabylon', 'FemmeThoughts', 'dirtbagcenter', 'opencarry', 'PhonesAreBad', 'MyrtleBeach', 'giftedstudents', 'TeenagersButBetter', 'RoyaleHighHaloTrades', 'FancyFollicles', 'Queer_Club', 'QueerWomenOfColor', 'SyrianCirclejerkWar', 'CrazyHand', 'PolskaPolityka', 'ThePortlandLeft', 'GentleDungeon', 'OSU', 'RobloxTradingandTea', 'AnimeGacha', 'TheBigWasteland', 'DanganronpaMEPs', 'CuteInnocentFun', 'Gachafanart', 'SouthernTransGang', 'BobDylanCircleJerk', 'CanadaPride', 'translation', 'SpeedOfLobsters', 'SexAble', 'MiloMurphysLaw', 'TPUSACirclejerk', 'CampingandHiking', 'Foamed', 'gender', 'everything_roblox', 'MockTheAltRight', 'TransCA', 'extroverts', 'LGBDropTheT', 'Enbymemes', 'MutualSupport', 'Makeup', 'assworship', 'Cultural_Marxism_irl', 'TeenagersCircleJerk', 'FtMteenagers', 'TopSurgery', 'DisabledBall', 'Witch', 'Askasurvivor', 'ValidationStation', 'FaceOfAce', 'photography', 'TraaButNoCommies', 'Bringbackdragons', 'fatpeoplestories', 'TheKikoDiner', 'ultravioletYT', 'SmashBrosUltimate', 'TruTalk', 'cross_tradingrblx', 'GachaClubAndroid', 'GODisGood7777777', 'DenverCirclejerk', 'Inktober', 'AlcheMage_TavernInn', 'RobloxMiddleMan', 'BokuNoShipAcademia', 'INFPmemes', 'linkiscute', 'qatar', 'SLAVONFIRE', 'thebearbubb', 'TopsAndBottoms', 'traumacore', 'u_TheEdenCrazy', 'Taki', 'kpopfanfiction', 'u_mazotori', 'gorillaz', 'KimetsuNoYaiba', 'TransyPillowTalk', 'mac', 'transgendervegan', 'hoggit', 'yourmomshousepodcast', 'Underrated_RH', 'transdating', 'earnrobux', 'OnceUponATime', 'averageredditor', 'straightsasklgbt', 'vce', 'shitpolandballsays', 'Anki', '90DayFiance', 'Vaping101', 'nintendo', 'WonderTrade', 'uofm', 'BrandNewSentence', 'Mysticat', 'RoyaleHigh_Stuff']
monthly_results = dict()

def reset_reservoir_and_results():
    'returns an empty reservoir for the subs we are interested in'
    reservoir = dict()
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