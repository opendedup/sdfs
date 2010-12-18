package org.opendedup.collections;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link ConcurrentMap} with a doubly-linked list running through its
 * entries.
 * <p>
 * This class provides the same semantics as a {@link ConcurrentHashMap} in
 * terms of iterators, acceptable keys, and concurrency characteristics, but
 * perform slightly worse due to the added expense of maintaining the linked
 * list. It differs from {@link java.util.LinkedHashMap} in that it does not
 * provide predictable iteration order.
 * <p>
 * This map is intended to be used for caches and provides the following
 * eviction policies:
 * <ul>
 * <li>First-in, First-out: Also known as insertion order. This policy has
 * excellent concurrency characteristics and an adequate hit rate.
 * <li>Second-chance: An enhanced FIFO policy that marks entries that have been
 * retrieved and saves them from being evicted until the next pass. This
 * enhances the FIFO policy by making it aware of "hot" entries, which increases
 * its hit rate to be equal to an LRU's under normal workloads. In the worst
 * case, where all entries have been saved, this policy degrades to a FIFO.
 * <li>Least Recently Used: An eviction policy based on the observation that
 * entries that have been used recently will likely be used again soon. This
 * policy provides a good approximation of an optimal algorithm, but suffers by
 * being expensive to maintain. The cost of reordering entries on the list
 * during every access operation reduces the concurrency and performance
 * characteristics of this policy.
 * </ul>
 *
 * @author <a href="mailto:ben.manes@reardencommerce.com">Ben Manes</a>
 * @see http://code.google.com/p/concurrentlinkedhashmap/wiki/ProductionVersion
 */
public final class ProductionMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<
    K, V>, Serializable {
  
  private static final long serialVersionUID = 8350170357874293408L;
  final ConcurrentMap<K, Node<K, V>> data;
  final EvictionListener<K, V> listener;
  final AtomicInteger capacity;
  final EvictionPolicy policy;
  final AtomicInteger length;
  final Node<K, V> sentinel;
  final Lock lock;

  @SuppressWarnings("unchecked")
  public ProductionMap(EvictionPolicy policy,int initialCapacity, int maximumCapacity,int concurrencyLevel,EvictionListener<K, V> listener) {
    this.data = new ConcurrentHashMap<K, Node<K, V>>(
        initialCapacity,  0.75f,concurrencyLevel);
    this.capacity = new AtomicInteger(maximumCapacity);
    this.listener = listener;
    this.length = new AtomicInteger();
    this.lock = new ReentrantLock();
    this.sentinel = new Node<K, V>(lock);
    this.policy = policy;
  }

  /**
   * Determines whether the map has exceeded its capacity.
   *
   * @return Whether the map has overflowed and an entry should be evicted.
   */
  private boolean isOverflow() {
    return size() > capacity();
  }

  /**
   * Sets the maximum capacity of the map and eagerly evicts entries until it
   * shrinks to the appropriate size.
   *
   * @param capacity The maximum capacity of the map.
   */
  public void setCapacity(int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException();
    }
    this.capacity.set(capacity);
    while (evict()) {
    }
  }

  /**
   * Retrieves the maximum capacity of the map.
   *
   * @return The maximum capacity.
   */
  public int capacity() {
    return capacity.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    int size = length.get();
    return (size >= 0) ? size : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    for (K key : keySet()) {
      remove(key);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(Object key) {
    return data.containsKey(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsValue(Object value) {
    if (value == null) {
      throw new IllegalArgumentException();
    }
    return data.containsValue(new Node<Object, Object>(null, value, null, lock));
  }

  /**
   * Evicts a single entry if the map exceeds the maximum capacity.
   */
  private boolean evict() {
    while (isOverflow()) {
      Node<K, V> node = sentinel.getNext();
      if (node == sentinel) {
        return false;
      } else if (policy.onEvict(this, node)) {
        // Attempt to remove the node if it's still available
        if (data.remove(node.getKey(), new Identity(node))) {
          length.decrementAndGet();
          node.remove();
          listener.onEviction(node.getKey(), node.getValue());
          return true;
        }
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(Object key) {
    Node<K, V> node = data.get(key);
    if (node != null) {
      policy.onAccess(this, node);
      return node.getValue();
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V put(K key, V value) {
    if (value == null) {
      throw new IllegalArgumentException();
    }
    Node<K, V> old = putIfAbsent(new Node<K, V>(key, value, sentinel, lock));
    return (old == null) ? null : old.getAndSetValue(value);
  }

  /**
   * {@inheritDoc}
   */
  public V putIfAbsent(K key, V value) {
    if (value == null) {
      throw new IllegalArgumentException();
    }
    Node<K, V> old = putIfAbsent(new Node<K, V>(key, value, sentinel, lock));
    return (old == null) ? null : old.getValue();
  }

  /**
   * Adds a node to the list and data store if it does not already exist.
   *
   * @param node An unlinked node to add.
   * @return The previous value in the data store.
   */
  private Node<K, V> putIfAbsent(Node<K, V> node) {
    Node<K, V> old = data.putIfAbsent(node.getKey(), node);
    if (old == null) {
      length.incrementAndGet();
      node.appendToTail();
      evict();
    } else {
      policy.onAccess(this, old);
    }
    return old;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V remove(Object key) {
    Node<K, V> node = data.remove(key);
    if (node == null) {
      return null;
    }
    length.decrementAndGet();
    node.remove();
    return node.getValue();
  }

  /**
   * {@inheritDoc}
   */
  public boolean remove(Object key, Object value) {
    Node<K, V> node = data.get(key);
    if ((node != null) && node.value.equals(value) && data.remove(key, new Identity(node))) {
      length.decrementAndGet();
      node.remove();
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public V replace(K key, V value) {
    if (value == null) {
      throw new IllegalArgumentException();
    }
    Node<K, V> node = data.get(key);
    return (node == null) ? null : node.getAndSetValue(value);
  }

  /**
   * {@inheritDoc}
   */
  public boolean replace(K key, V oldValue, V newValue) {
    if (newValue == null) {
      throw new IllegalArgumentException();
    }
    Node<K, V> node = data.get(key);
    return (node == null) ? false : node.casValue(oldValue, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<K> keySet() {
    return new KeySet();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<V> values() {
    return new Values();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Entry<K, V>> entrySet() {
    return new EntrySet();
  }

  /**
   * A listener registered for notification when an entry is evicted.
   */
  public interface EvictionListener<K, V> {

    /**
     * A call-back notification that the entry was evicted.
     *
     * @param key The evicted key.
     * @param value The evicted value.
     */
    void onEviction(K key, V value);
  }

  /**
   * The replacement policy to apply to determine which entry to discard when
   * the capacity has been reached.
   */
  public enum EvictionPolicy {

    /**
     * Evicts entries based on insertion order.
     */
    FIFO() {
      @Override
      <K, V> void onAccess(ProductionMap<K, V> map, Node<K, V> node) {
        // do nothing
      }

      @Override
      <K, V> boolean onEvict(ProductionMap<K, V> map, Node<K, V> node) {
        return true;
      }
    },

    /**
     * Evicts entries based on insertion order, but gives an entry a "second
     * chance" if it has been requested recently.
     */
    SECOND_CHANCE() {
      @Override
      <K, V> void onAccess(ProductionMap<K, V> map, Node<K, V> node) {
        node.setMarked(true);
      }

      @Override
      <K, V> boolean onEvict(ProductionMap<K, V> map, Node<K, V> node) {
        if (node.isMarked()) {
          node.moveToTail();
          node.setMarked(false);
          return false;
        }
        return true;
      }
    },

    /**
     * Evicts entries based on how recently they are used, with the least recent
     * evicted first.
     */
    LRU() {
      @Override
      <K, V> void onAccess(ProductionMap<K, V> map, Node<K, V> node) {
        node.moveToTail();
      }

      @Override
      <K, V> boolean onEvict(ProductionMap<K, V> map, Node<K, V> node) {
        return true;
      }
    };

    /**
     * Performs any operations required by the policy after a node was
     * successfully retrieved.
     */
    abstract <K, V> void onAccess(ProductionMap<K, V> map, Node<K, V> node);

    /**
     * Determines whether to evict the node at the head of the list.
     */
    abstract <K, V> boolean onEvict(ProductionMap<K, V> map, Node<K, V> node);
  }

  /**
   * A node on the double-linked list. This list cross-cuts the data store.
   */
  @SuppressWarnings("unchecked")
  static final class Node<K, V> implements Serializable {
    private static final long serialVersionUID = 1461281468985304519L;
    private static final AtomicReferenceFieldUpdater<Node, Object> valueUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Object.class, "value");
    private static final Node UNLINKED = new Node(null);

    private final K key;
    private final Lock lock;
    private final Node<K, V> sentinel;

    private volatile V value;
    private volatile boolean marked;
    private volatile Node<K, V> prev;
    private volatile Node<K, V> next;

    /**
     * Creates a new sentinel node.
     */
    public Node(Lock lock) {
      this.sentinel = this;
      this.value = null;
      this.lock = lock;
      this.prev = this;
      this.next = this;
      this.key = null;
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(K key, V value, Node<K, V> sentinel, Lock lock) {
      this.sentinel = sentinel;
      this.next = UNLINKED;
      this.prev = UNLINKED;
      this.value = value;
      this.lock = lock;
      this.key = key;
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToTail() {
      lock.lock();
      try {
        // Allow moveToTail() to no-op or removal to spin-wait
        next = sentinel;

        // Read the tail on the stack to avoid unnecessary volatile reads
        final Node<K, V> tail = sentinel.prev;
        sentinel.prev = this;
        tail.next = this;
        prev = tail;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Removes the node from the list.
     * <p>
     * If the node has not yet been appended to the tail it will wait for that
     * operation to complete.
     */
    public void remove() {
      for (;;) {
        if (isUnlinked()) {
          continue; // await appendToTail()
        }

        lock.lock();
        try {
          if (isUnlinked()) {
            continue; // await appendToTail()
          }
          prev.next = next;
          next.prev = prev;
          next = UNLINKED; // mark as unlinked
        } finally {
          lock.unlock();
        }
        return;
      }
    }

    /**
     * Moves the node to the tail.
     * <p>
     * If the node has been unlinked or is already at the tail, no-ops.
     */
    public void moveToTail() {
      if (isTail() || isUnlinked()) {
        return;
      }
      lock.lock();
      try {
        if (isTail() || isUnlinked()) {
          return;
        }
        // unlink
        prev.next = next;
        next.prev = prev;

        // link
        next = sentinel; // ordered for isTail()
        prev = sentinel.prev;
        sentinel.prev = this;
        prev.next = this;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Checks whether the node is linked on the list chain.
     *
     * @return Whether the node has not yet been linked on the list.
     */
    public boolean isUnlinked() {
      return (next == UNLINKED);
    }

    /**
     * Checks whether the node is the last linked on the list chain.
     *
     * @return Whether the node is at the tail of the list.
     */
    public boolean isTail() {
      return (next == sentinel);
    }

    /*
     * Key operators
     */
    public K getKey() {
      return key;
    }

    /*
     * Value operators
     */
    public V getValue() {
      return (V) valueUpdater.get(this);
    }

    public V getAndSetValue(V value) {
      return (V) valueUpdater.getAndSet(this, value);
    }

    public boolean casValue(V expect, V update) {
      return valueUpdater.compareAndSet(this, expect, update);
    }

    /*
     * Previous node operators
     */
    public Node<K, V> getPrev() {
      return prev;
    }

    /*
     * Next node operators
     */
    public Node<K, V> getNext() {
      return next;
    }

    /*
     * Access frequency operators
     */
    public boolean isMarked() {
      return marked;
    }

    public void setMarked(boolean marked) {
      this.marked = marked;
    }

    /**
     * Only ensures that the values are equal, as the key may be <tt>null</tt>
     * for look-ups.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof Node)) {
        return false;
      }
      V value = getValue();
      Node<?, ?> node = (Node<?, ?>) obj;
      return (value == null) ? (node.getValue() == null) : value.equals(node.getValue());
    }

    @Override
    public int hashCode() {
      return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
    }
  }

  /**
   * Allows {@link #equals(Object)} to compare using object identity.
   */
  private static final class Identity {
    private final Object delegate;

    public Identity(Object delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean equals(Object o) {
      return (o == delegate);
    }
  }

  /**
   * An adapter to safely externalize the keys.
   */
  private final class KeySet extends AbstractSet<K> {
    private final ProductionMap<K, V> map = ProductionMap.this;

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public Iterator<K> iterator() {
      return new KeyIterator();
    }

    @Override
    public boolean contains(Object obj) {
      return map.containsKey(obj);
    }

    @Override
    public boolean remove(Object obj) {
      return (map.remove(obj) != null);
    }

    @Override
    public Object[] toArray() {
      return map.data.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] array) {
      return map.data.keySet().toArray(array);
    }
  }

  /**
   * An adapter to safely externalize the keys.
   */
  private final class KeyIterator implements Iterator<K> {
    private final EntryIterator iterator =
        new EntryIterator(ProductionMap.this.data.values().iterator());

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public K next() {
      return iterator.next().getKey();
    }

    public void remove() {
      iterator.remove();
    }
  }

  /**
   * An adapter to represent the data store's values in the external type.
   */
  private final class Values extends AbstractCollection<V> {
    private final ProductionMap<K, V> map = ProductionMap.this;

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public Iterator<V> iterator() {
      return new ValueIterator();
    }

    @Override
    public boolean contains(Object o) {
      return map.containsValue(o);
    }

    @Override
    public Object[] toArray() {
      Collection<V> values = new ArrayList<V>(size());
      for (V value : this) {
        values.add(value);
      }
      return values.toArray();
    }

    @Override
    public <T> T[] toArray(T[] array) {
      Collection<V> values = new ArrayList<V>(size());
      for (V value : this) {
        values.add(value);
      }
      return values.toArray(array);
    }
  }

  /**
   * An adapter to represent the data store's values in the external type.
   */
  private final class ValueIterator implements Iterator<V> {
    private final EntryIterator iterator =
        new EntryIterator(ProductionMap.this.data.values().iterator());

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public V next() {
      return iterator.next().getValue();
    }

    public void remove() {
      iterator.remove();
    }
  }

  /**
   * An adapter to represent the data store's entry set in the external type.
   */
  private final class EntrySet extends AbstractSet<Entry<K, V>> {
    private final ProductionMap<K, V> map = ProductionMap.this;

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return new EntryIterator(map.data.values().iterator());
    }

    @Override
    public boolean contains(Object obj) {
      if (!(obj instanceof Entry)) {
        return false;
      }
      Entry<?, ?> entry = (Entry<?, ?>) obj;
      Node<K, V> node = map.data.get(entry.getKey());
      return (node != null) && (node.value.equals(entry.getValue()));
    }

    @Override
    public boolean add(Entry<K, V> entry) {
      return (map.putIfAbsent(entry.getKey(), entry.getValue()) == null);
    }

    @Override
    public boolean remove(Object obj) {
      if (!(obj instanceof Entry)) {
        return false;
      }
      Entry<?, ?> entry = (Entry<?, ?>) obj;
      return map.remove(entry.getKey(), entry.getValue());
    }

    @Override
    public Object[] toArray() {
      Collection<Entry<K, V>> entries = new ArrayList<Entry<K, V>>(size());
      for (Entry<K, V> entry : this) {
        entries.add(new SimpleEntry<K, V>(entry));
      }
      return entries.toArray();
    }

    @Override
    public <T> T[] toArray(T[] array) {
      Collection<Entry<K, V>> entries = new ArrayList<Entry<K, V>>(size());
      for (Entry<K, V> entry : this) {
        entries.add(new SimpleEntry<K, V>(entry));
      }
      return entries.toArray(array);
    }
  }

  /**
   * An adapter to represent the data store's entry iterator in the external
   * type.
   */
  private final class EntryIterator implements Iterator<Entry<K, V>> {
    private final Iterator<Node<K, V>> iterator;
    private Entry<K, V> current;

    public EntryIterator(Iterator<Node<K, V>> iterator) {
      this.iterator = iterator;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public Entry<K, V> next() {
      current = new NodeEntry(iterator.next());
      return current;
    }

    public void remove() {
      if (current == null) {
        throw new IllegalStateException();
      }
      ProductionMap.this.remove(current.getKey(), current.getValue());
      current = null;
    }
  }

  /**
   * An entry that is tied to the map instance to allow updates through the
   * entry or the map to be visible.
   */
  private final class NodeEntry implements Entry<K, V> {
    private final ProductionMap<K, V> map = ProductionMap.this;
    private final Node<K, V> node;

    public NodeEntry(Node<K, V> node) {
      this.node = node;
    }

    public K getKey() {
      return node.getKey();
    }

    public V getValue() {
      if (node.isUnlinked()) {
        V value = map.get(getKey());
        if (value != null) {
          return value;
        }
      }
      return node.getValue();
    }

    public V setValue(V value) {
      return map.replace(getKey(), value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof Entry)) {
        return false;
      }
      Entry<?, ?> entry = (Entry<?, ?>) obj;
      return eq(getKey(), entry.getKey()) && eq(getValue(), entry.getValue());
    }

    @Override
    public int hashCode() {
      K key = getKey();
      V value = getValue();
      return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
    }

    @Override
    public String toString() {
      return getKey() + "=" + getValue();
    }

    private boolean eq(Object o1, Object o2) {
      return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }
  }

  /**
   * This duplicates {@link java.util.AbstractMap.SimpleEntry} until the class
   * is made accessible (public in JDK-6).
   */
  private static class SimpleEntry<K, V> implements Entry<K, V> {
    private final K key;
    private V value;

    public SimpleEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public SimpleEntry(Entry<K, V> e) {
      this.key = e.getKey();
      this.value = e.getValue();
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public V setValue(V value) {
      V oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof Entry)) {
        return false;
      }
      Entry<?, ?> entry = (Entry<?, ?>) obj;
      return eq(key, entry.getKey()) && eq(value, entry.getValue());
    }

    @Override
    public int hashCode() {
      return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
    }

    @Override
    public String toString() {
      return key + "=" + value;
    }

    private static boolean eq(Object o1, Object o2) {
      return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }
  }
}
