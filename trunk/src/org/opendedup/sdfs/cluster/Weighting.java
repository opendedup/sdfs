package org.opendedup.sdfs.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class Weighting {

	int value;
	int weighting;

	public Weighting(int v, int w) {
		this.value = v;
		this.weighting = w;
	}

	public static int weightedRandom(List<Weighting> weightingOptions) {

		// determine sum of all weightings
		int total = 0;
		for (Weighting w : weightingOptions) {
			total += w.weighting;
		}

		// select a random value between 0 and our total
		int random = new Random().nextInt(total);

		// loop thru our weightings until we arrive at the correct one
		int current = 0;
		for (Weighting w : weightingOptions) {
			current += w.weighting;
			if (random < current)
				return w.value;
		}

		// shouldn't happen.
		return -1;
	}

	public static void main(String[] args) {

		List<Weighting> weightings = new ArrayList<Weighting>();
		weightings.add(new Weighting(0, 7));
		weightings.add(new Weighting(1, 1));
		weightings.add(new Weighting(2, 2));
		int zct = 0;
		int oct = 0;
		int tct = 0;
		for (int i = 0; i < 10000; i++) {
			int n = weightedRandom(weightings);
			if (n == 0)
				zct++;
			if (n == 1)
				oct++;
			if (n == 2)
				tct++;
		}
		System.out.println(zct);
		System.out.println(oct);
		System.out.println(tct);
	}
}
