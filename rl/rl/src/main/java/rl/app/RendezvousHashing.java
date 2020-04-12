package rl.app;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class RendezvousHashing<K, N extends Comparable<? super N>> {
	private final Funnel<K> keyFunnel;
	private final Funnel<N> nodeFunnel;
	private final ConcurrentSkipListSet<N> nodes;

    /**
     * Test usage only.
     * This implementation RendezvousHashing 
     */
	/**
	 * Creates a new  with a starting set of nodes provided by init. The funnels will be used when generating the hash that combines the nodes and
	 * keys. The hasher specifies the hashing algorithm to use.
	 */
	public RendezvousHashing(Funnel<K> keyFunnel, Funnel<N> nodeFunnel, Collection<N> init) {
		this.keyFunnel = keyFunnel;
		this.nodeFunnel = nodeFunnel;
		this.nodes = new ConcurrentSkipListSet<N>(init);
	}

	/**
	 * Removes a node from the pool. Keys that referenced it should after this be evenly distributed amongst the other nodes
	 */
	public boolean removeNode(N node) {
		return nodes.remove(node);
	}

	/**
	 * Add a new node to pool and take an even distribution of the load off existing nodes
	 */
	public boolean addNode(N node) {
		return nodes.add(node);
	}

	/**
	 * return a node for a given key
	 */
	public N getHash(K key) {
		long maxValue = Long.MIN_VALUE;
		N max = null;
		for (N node : nodes) {
			long nodesHash = Hashing.murmur3_128().newHasher()
					.putObject(key, keyFunnel)
					.putObject(node, nodeFunnel)
					.hash().asLong();
			if (nodesHash > maxValue) {
				max = node;
				maxValue = nodesHash;
			}
		}
		return max;
	}
}