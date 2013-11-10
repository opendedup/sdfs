package org.opendedup.sdfs.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WeightedRandomServer implements ServerWeighting {
	List<DSEServerWeighting> weights = null;
	int arsz = 0;
	private int total = 0;
	Random rnd = new Random();

	@Override
	public void init(List<DSEServerWeighting> weights) {
		this.weights = weights;
		for (DSEServerWeighting w : weights) {
			total += w.weighting;
		}
		arsz = weights.size();

	}

	@Override
	public DSEServer getServer(byte[] ignoredHosts) {
		int random = rnd.nextInt(total);
		DSEServer s = null;
		// loop thru our weightings until we arrive at the correct one
		int current = 0;
		if (weights.size() == 0)
			return null;
		while (s == null) {
			for (DSEServerWeighting w : weights) {
				current += w.weighting;
				if (random < current) {
					if (!this.ignoreHost(w.server, ignoredHosts)) {
						s = w.server;
					}
					break;
				}
			}
		}
		return s;
	}

	@Override
	public List<DSEServer> getServers(int sz, byte[] ignoredHosts) {
		int random = rnd.nextInt(total);
		ArrayList<DSEServer> lst = new ArrayList<DSEServer>();
		if (sz >= arsz) {
			for (DSEServerWeighting w : weights) {
				if (!this.ignoreHost(w.server, ignoredHosts))
					lst.add(w.server);
			}
		} else {
			// loop thru our weightings until we arrive at the correct one
			int current = 0;
			int pos = 0;
			for (DSEServerWeighting w : weights) {
				current += w.weighting;
				if (random < current)
					break;
				else
					pos++;
			}

			int added = 0;
			for (int i = pos; i < arsz; i++) {
				DSEServer s = weights.get(i).server;
				if (!this.ignoreHost(s, ignoredHosts)) {
					lst.add(s);
					added++;
					if (added >= sz)
						break;
				}
			}
			if (added < sz) {
				for (int i = 0; i < arsz; i++) {
					DSEServer s = weights.get(i).server;
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

}
