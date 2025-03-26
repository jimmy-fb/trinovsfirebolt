SELECT MEDIAN(FNV_HASH(id, FNV_HASH(agentname, FNV_HASH(operatingsystem, FNV_HASH(devicearch, FNV_HASH(browser)))))) FROM agents;
SELECT MEDIAN(FNV_HASH(ip, FNV_HASH(autonomoussystem, FNV_HASH(asname)))) FROM ipaddresses;
SELECT MEDIAN(FNV_HASH(word, FNV_HASH(word_hash, FNV_HASH(word_id, FNV_HASH(firstseen, FNV_HASH(is_topic)))))) FROM searchwords;
SELECT MEDIAN(FNV_HASH(pageurl, FNV_HASH(pagerank, FNV_HASH(avgduration)))) FROM rankings;