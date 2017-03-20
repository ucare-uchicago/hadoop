package org.apache.hadoop.hdfs.protocol;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class SimpleStat {
  private long min, max, sum, count;
  private ArrayList<Long> data;

  public SimpleStat() {
    min = 1000000;
    max = -1000000;
    sum = 0;
    count = 0;
    data = new ArrayList<Long>();
  }

  public synchronized void addValue(long val) {
    data.add(val);
    if (min > val)
      min = val;
    if (max < val)
      max = val;
    sum += val;
    count += 1;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long getSum() {
    return sum;
  }

  public long getCount() {
    return count;
  }

  public double getAvg() {
    return count > 0 ? (double) sum / count : 0;
  }

  @Override
  public String toString() {
    String ret = "min = " + getMin() + "\nmax = " + getMax()
                 + "\navg = " + getAvg() + "\nsum = " + getSum()
                 + "\ncount = " + getCount() + "\n";
    return ret;
  }
  
  public Map<Long,Integer> getFrequency() {
    TreeMap<Long,Integer> freq = new TreeMap<Long,Integer>();
    for (Long val : data) {
      if (!freq.containsKey(val)) {
        freq.put(val,1);
      } else {
        int ct = freq.get(val)+1;
        freq.put(val,ct);
      }
    }
    return freq;
  }
  
  public String getCDFDataString(){
    StringBuilder sb = new StringBuilder();
    Map<Long,Integer> freq = getFrequency();
    double increment = 1.0 / getCount();
    double rollSum = 0.0;
    sb.append("0 " + rollSum + "\n");
    for (Map.Entry<Long,Integer> count : freq.entrySet()) {
      for (int i=0; i<count.getValue(); i++) {
        rollSum += increment;
        sb.append(count.getKey() + " " + rollSum + "\n");
      }
    }
    return sb.toString();
  }
}
