package rl.app;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

public class LoadBalance {
    private static final int nodesCount = 1000;
    private static final int keyCount = 1000000;
    private static final Funnel<CharSequence> charsFunnel = Funnels.stringFunnel(Charset.defaultCharset());
    public static void main(String[] args) {
        System.out.println("RL Test: Consistent Hashing v.s. Rendezvous Hashing\n");
        System.out.println("Nodes Count: " + nodesCount);
        System.out.println("Keys Count: " + keyCount + "\n");

        System.out.println("### Consistent Hashing ###");
        testConsistentHashing();

        System.out.println("\n### Rendezvous Hashing ###");
        testRendezvousHashing();
    }

    private static void testConsistentHashing() {
        // initiate nodes for Consistent Hashing
        Map<String, AtomicInteger> CHNodesMap = Maps.newHashMap();
        List<String> CHNodes = Lists.newArrayList();
        for(int i = 0 ; i < nodesCount; i ++) {
            CHNodes.add("CHNode"+i);
            CHNodesMap.put("CHNode"+i, new AtomicInteger());
        }
        ConsistentHashing<String, String> ch = new ConsistentHashing(charsFunnel, charsFunnel, CHNodes);
        
        // insert keys
        long startTime = System.currentTimeMillis();
        for(int i = 0 ; i < keyCount; i++) {
            CHNodesMap.get(ch.getHash(""+i)).incrementAndGet();
        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        // System.out.println("Execution Time in milliseconds : " + timeElapsed);

        // print out distriubution + clear
        for(Entry<String, AtomicInteger> entry : CHNodesMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().get());
            entry.getValue().set(0);
        }
        
        // remove node 3
        System.out.println("\n=== After Removing Node 3 ===");
        ch.removeNode("CHNode3");
        CHNodesMap.remove("CHNode3");
    
        // re-add
        for(int i = 0 ; i < keyCount; i++) {
            CHNodesMap.get(ch.getHash(""+i)).incrementAndGet();
        }
        
        // print out distriubution again
        for(Entry<String, AtomicInteger> entry : CHNodesMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().get());
        }
    }

    private static void testRendezvousHashing() {
        // initiate nodes for Rendezvous Hashing
        Map<String, AtomicInteger> HRWNodesMap = Maps.newHashMap();
        List<String> HRWNodes = Lists.newArrayList();
        for(int i = 0 ; i < nodesCount; i ++) {
            HRWNodes.add("HRWNode"+i);
            HRWNodesMap.put("HRWNode"+i, new AtomicInteger());
        }
        RendezvousHashing<String, String> hrw = new RendezvousHashing(charsFunnel, charsFunnel, HRWNodes);
    
        // insert keys
        long startTime = System.currentTimeMillis();
        for(int i = 0 ; i < keyCount; i++) {
            HRWNodesMap.get(hrw.getHash(""+i)).incrementAndGet();
        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        // System.out.println("Execution Time in milliseconds : " + timeElapsed);
    

        // print out distriubution + clear
        for(Entry<String, AtomicInteger> entry : HRWNodesMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().get());
            entry.getValue().set(0);
        }

        // remove node 3
        System.out.println("\n=== After Removing Node 3 ===");
        hrw.removeNode("HRWNode3");
        HRWNodesMap.remove("HRWNode3");

        // re-add
        for(int i = 0 ; i < keyCount; i++) {
            HRWNodesMap.get(hrw.getHash(""+i)).incrementAndGet();
        }

        // print out distriubution again
        for(Entry<String, AtomicInteger> entry : HRWNodesMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().get());
        }
    }
}