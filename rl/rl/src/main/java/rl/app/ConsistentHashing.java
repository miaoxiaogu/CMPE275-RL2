package rl.app;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;

/**
 * Test usage only.
 * This implementation doesn't add the virtual replicas improvement to mitigate issues with uneven distribution.
 */
public class ConsistentHashing<K, N> {

	private final SortedMap<Long, N> ring = new TreeMap<Long, N>();
	private Funnel<N> nodeFunnel;
	private Funnel<K> keyFunnel;

	public ConsistentHashing(Funnel<K> keyFunnel, Funnel<N> nodeFunnel, Collection<N> nodes) {
		this.nodeFunnel = nodeFunnel;
		this.keyFunnel = keyFunnel;
		for (N node : nodes) {
			addNode(node);
		}
	}

	public boolean addNode(N node) { 
		ring.put(Hashing.murmur3_128().newHasher().putObject(node, nodeFunnel).hash().asLong(), node); 
		return true;
	}

	public boolean removeNode(N node) {
		return node == ring.remove(Hashing.murmur3_128().newHasher().putObject(node, nodeFunnel).hash().asLong()); 
	}

	public N getHash(K key) { 
		Long hash = Hashing.murmur3_128().newHasher().putObject(key, keyFunnel).hash().asLong();
		if (!ring.containsKey(hash)) {
			SortedMap<Long, N> tailMap = ring.tailMap(hash);
			hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
		}
		return ring.get(hash);
	}
}
