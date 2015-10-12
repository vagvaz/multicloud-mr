package eu.leads.processor.common.continuous;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by anydriotis on 10/5/15.
 */

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * Created by vagvaz on 10/5/15.
 */
public class ConcurrentDiskQueue implements java.util.Queue {

  BlockingQueue queue;
  int threshold = 0;
  DB db;

  public ConcurrentDiskQueue(int blockSize) {
    this.threshold = blockSize;
    db = DBMaker.tempFileDB()
        .deleteFilesAfterClose()
        .closeOnJvmShutdown()
        .transactionDisable()
        .make();
    queue = db.getQueue("fifo");
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return queue.contains(o);
  }

  @Override
  public Iterator iterator() {
    return queue.iterator();
  }

  @Override
  public Object[] toArray() {
    return queue.toArray();
  }

  @Override
  public Object[] toArray(Object[] a) {
    return queue.toArray(a);
  }

  @Override
  public boolean add(Object o) {
    return queue.add(o);
  }

  @Override
  public boolean remove(Object o) {
    return queue.remove(o);
  }

  @Override
  public boolean addAll(Collection c) {
    return queue.addAll(c);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean retainAll(Collection c) {
    return queue.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean containsAll(Collection c) {
    return queue.containsAll(c);
  }

  @Override
  public boolean offer(Object o) {
    return queue.offer(o);
  }

  @Override
  public Object remove() {
    return queue.remove();
  }

  @Override
  public Object poll() {
    return queue.poll();
  }

  @Override
  public Object element() {
    return queue.element();
  }

  @Override
  public Object peek() {
    return queue.peek();
  }
}
