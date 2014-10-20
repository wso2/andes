package org.dna.mqtt.moquette.messaging.spi.impl.subscriptions;

import org.dna.mqtt.moquette.messaging.spi.IPersistentSubscriptionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class SubscriptionsStore {
    
    public static interface IVisitor<T> {
        void visit(TreeNode node);
        
        T getResult();
    }
    
    private class DumpTreeVisitor implements IVisitor<String> {
        
        String s = "";

        public void visit(TreeNode node) {
            String subScriptionsStr = "";
            for (Subscription sub : node.m_subscriptions) {
                subScriptionsStr += sub.toString();
            }
            s += node.getToken() == null ? "" : node.getToken().toString();
            s += subScriptionsStr + "\n";
        }
        
        public String getResult() {
            return s;
        }
    }
    
    private class SubscriptionTreeCollector implements IVisitor<List<Subscription>> {
        
        private List<Subscription> m_allSubscriptions = new ArrayList<Subscription>();

        public void visit(TreeNode node) {
            m_allSubscriptions.addAll(node.subscriptions());
        }
        
        public List<Subscription> getResult() {
            return m_allSubscriptions;
        }
    }

    private TreeNode subscriptions = new TreeNode(null);
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionsStore.class);

    private IPersistentSubscriptionStore m_storageService;

    /**
     * Initialize basic store structures, like the FS storage to maintain
     * client's topics subscriptions
     */
    public void init(IPersistentSubscriptionStore storageService) {
        LOG.debug("init invoked");

        m_storageService = storageService;

        //reload any subscriptions persisted
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reloading all stored subscriptions...subscription tree before {}", dumpTree());
        }
        
        for (Subscription subscription : m_storageService.retrieveAllSubscriptions()) {
            LOG.debug("Re-subscribing {} to topic {}", subscription.getClientId(), subscription.getTopic());
            addDirect(subscription);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading. Subscription tree after {}", dumpTree());
        }
    }
    
    protected void addDirect(Subscription newSubscription) {
        TreeNode current = findMatchingNode(newSubscription.topic);
        current.addSubscription(newSubscription);
    }
    
    private TreeNode findMatchingNode(String topic) {
        List<Token> tokens = new ArrayList<Token>();
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
            LOG.error(null, ex);
//            return;
        }

        TreeNode current = subscriptions;
        for (Token token : tokens) {
            TreeNode matchingChildren;

            //check if a children with the same token already exists
            if ((matchingChildren = current.childWithToken(token)) != null) {
                current = matchingChildren;
            } else {
                //create a new node for the newly inserted token
                matchingChildren = new TreeNode(current);
                matchingChildren.setToken(token);
                current.addChild(matchingChildren);
                current = matchingChildren;
            }
        }
        return current;
    }

    public void add(Subscription newSubscription) {
        addDirect(newSubscription);

        //log the subscription
        String clientID = newSubscription.getClientId();
        m_storageService.addNewSubscription(newSubscription, clientID);
    }


    public void removeSubscription(String topic, String clientID) {
        TreeNode matchNode = findMatchingNode(topic);
        
        //search for the subscription to remove
        Subscription toBeRemoved = null;
        for (Subscription sub : matchNode.subscriptions()) {
            if (sub.topic.equals(topic) && sub.getClientId().equals(clientID)) {
                toBeRemoved = sub;
                break;
            }
        }
        
        if (toBeRemoved != null) {
            matchNode.subscriptions().remove(toBeRemoved);
        }
    }
    
    /**
     * TODO implement testing
     */
    public void clearAllSubscriptions() {
        SubscriptionTreeCollector subsCollector = new SubscriptionTreeCollector();
        bfsVisit(subscriptions, subsCollector);
        
        List<Subscription> allSubscriptions = subsCollector.getResult();
        for (Subscription subscription : allSubscriptions) {
            removeSubscription(subscription.getTopic(), subscription.getClientId());
        }
    }

    /**
     * Visit the topics tree to remove matching subscriptions with clientID
     */
    public void removeForClient(String clientID) {
        subscriptions.removeClientSubscriptions(clientID);

        //remove from log all subscriptions
        m_storageService.removeAllSubscriptions(clientID);
    }

    public void deactivate(String clientID) {
        subscriptions.deactivate(clientID);
    }

    public void activate(String clientID) {
        LOG.debug("Activating subscriptions for clientID <{}>", clientID);
        subscriptions.activate(clientID);
    }

    /**
     * Given a topic string return the clients subscriptions that matches it.
     * Topic string can't contain character # and + because they are reserved to
     * listeners subscriptions, and not topic publishing.
     */
    public List<Subscription> matches(String topic) {
        List<Token> tokens;
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
            LOG.error(null, ex);
            return Collections.EMPTY_LIST;
        }

        Queue<Token> tokenQueue = new LinkedBlockingDeque<Token>(tokens);
        List<Subscription> matchingSubs = new ArrayList<Subscription>();
        subscriptions.matches(tokenQueue, matchingSubs);
        return matchingSubs;
    }

    public boolean contains(Subscription sub) {
        return !matches(sub.topic).isEmpty();
    }

    public int size() {
        return subscriptions.size();
    }
    
    public String dumpTree() {
        DumpTreeVisitor visitor = new DumpTreeVisitor();
        bfsVisit(subscriptions, visitor);
        return visitor.getResult();
    }
    
    private void bfsVisit(TreeNode node, IVisitor visitor) {
        if (node == null) {
            return;
        }
        visitor.visit(node);
        for (TreeNode child : node.m_children) {
            bfsVisit(child, visitor);
        }
    }
    
    /**
     * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
     */
    //TODO reimplement with iterators or with queues
    public static boolean matchTopics(String msgTopic, String subscriptionTopic) {
        try {
            List<Token> msgTokens = SubscriptionsStore.splitTopic(msgTopic);
            List<Token> subscriptionTokens = SubscriptionsStore.splitTopic(subscriptionTopic);
            int i = 0;
            Token subToken = null;
            for (; i< subscriptionTokens.size(); i++) {
                subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    if (i >= msgTokens.size()) {
                        return false;
                    }
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                    if (subToken == Token.SINGLE) {
                        //skip a step forward
                    }
                }
            }
            //if last token was a SINGLE then treat it as an empty
            if (subToken == Token.SINGLE && (i - msgTokens.size() == 1)) {
               i--; 
            }
            return i == msgTokens.size();
        } catch (ParseException ex) {
            LOG.error(null, ex);
            throw new RuntimeException(ex);
        }
    }
    
    protected static List<Token> splitTopic(String topic) throws ParseException {
        List res = new ArrayList<Token>();
        String[] splitted = topic.split("/");

        if (splitted.length == 0) {
            res.add(Token.EMPTY);
        }

        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
//                if (i != 0) {
//                    throw new ParseException("Bad format of topic, expetec topic name between separators", i);
//                }
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Token.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }
}
