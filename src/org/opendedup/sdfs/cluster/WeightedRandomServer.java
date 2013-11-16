package org.opendedup.sdfs.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.jgroups.Address;
import org.opendedup.logging.SDFSLogger;

public class WeightedRandomServer implements ServerWeighting {
	List<DSEServer> servers = null;
	int arsz = 0;
	private int total = 0;
	Random rnd = new Random();

	@Override
	public void init(List<DSEServer> servers) {
		this.servers = servers;
		double totsz = 0;
		for (DSEServer w : servers) {
			totsz += w.maxSize - w.currentSize;
		}
		if(totsz > 0) {
		for (DSEServer w : servers) {
			long avail = w.maxSize - w.currentSize;
			if(avail > 0) {
				double pt = (double)avail/totsz;
				w.weight = (int) Math.ceil((pt) * 100);
				SDFSLogger.getLog().debug("pt = " + pt + " avail = " + avail + " tot = " +totsz + " weight = " + w.weight);
				this.total += w.weight;
			}
		}
		Collections.sort(servers, new CustomComparator());
		arsz = servers.size();
		}
		SDFSLogger.getLog().debug("Created new weighted random with total " + total);
	}

	@Override
	public Address getAddress(byte[] ignoredHosts) {
		int random = rnd.nextInt(total);
		Address s = null;
		// loop thru our weightings until we arrive at the correct one
		int current = 0;
		if (servers.size() == 0)
			return null;
		while (s == null) {
			for (DSEServer w : servers) {
				current += w.weight;
				if (random < current) {
					if (!this.ignoreHost(w, ignoredHosts)) {
						s = w.address;
					}
					break;
				}
			}
		}
		return s;
	}

	@Override
	public List<Address> getAddresses(int sz, byte[] ignoredHosts) {
		int random = rnd.nextInt(total);
		ArrayList<Address> lst = new ArrayList<Address>();
		if (sz >= arsz) {
			for (DSEServer w : servers) {
				if (!this.ignoreHost(w, ignoredHosts))
					lst.add(w.address);
			}
		} else {
			// loop thru our weightings until we arrive at the correct one
			int current = 0;
			int pos = 0;
			for (DSEServer w : servers) {
				current += w.weight;
				if (random < current)
					break;
				else
					pos++;
			}

			int added = 0;
			for (int i = pos; i < arsz; i++) {
				DSEServer s = servers.get(i);
				if (!this.ignoreHost(s, ignoredHosts)) {
					lst.add(s.address);
					added++;
					if (added >= sz)
						break;
				}
			}
			if (added < sz) {
				for (int i = 0; i < arsz; i++) {
					DSEServer s = servers.get(i);
					if (!this.ignoreHost(s, ignoredHosts)) {
						lst.add(s.address);
						added++;
						if (added >= sz)
							break;
					}
				}

			}
		}
		return lst;
	}
	
	@Override
	public List<DSEServer> getServers(int sz, byte[] ignoredHosts) {
		int random = rnd.nextInt(total);
		ArrayList<DSEServer> lst = new ArrayList<DSEServer>();
		if (sz >= arsz) {
			for (DSEServer w : servers) {
				if (!this.ignoreHost(w, ignoredHosts))
					lst.add(w);
			}
		} else {
			// loop thru our weightings until we arrive at the correct one
			int current = 0;
			int pos = 0;
			for (DSEServer w : servers) {
				current += w.weight;
				if (random < current)
					break;
				else
					pos++;
			}

			int added = 0;
			for (int i = pos; i < arsz; i++) {
				DSEServer s = servers.get(i);
				if (!this.ignoreHost(s, ignoredHosts)) {
					lst.add(s);
					added++;
					if (added >= sz)
						break;
				}
			}
			if (added < sz) {
				for (int i = 0; i < arsz; i++) {
					DSEServer s = servers.get(i);
					if (!this.ignoreHost(s, ignoredHosts)) {
						lst.add(s);
						added++;
						if (added >= sz)
							break;
					}
				}

			}
		}
		return lst;
	}

	private boolean ignoreHost(DSEServer s, byte[] ignoredHosts) {
		if (ignoredHosts == null)
			return false;
		else {
			for (byte b : ignoredHosts) {
				if (b == s.id)
					return true;
			}
			return false;
		}
	}
	
	private class CustomComparator implements Comparator<DSEServer> {
		@Override
		public int compare(DSEServer o1, DSEServer o2) {
			int fs1 = o1.weight;
			int fs2 = o2.weight;
			if (fs1 > fs2)
				return 1;
			else if (fs1 < fs2)
				return -1;
			else
				return 0;
		}
	}

}
