/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class tWeighting {

	int value;
	int weighting;

	public tWeighting(int v, int w) {
		this.value = v;
		this.weighting = w;
	}

	public static int weightedRandom(List<tWeighting> weightingOptions) {

		// determine sum of all weightings
		int total = 0;
		for (tWeighting w : weightingOptions) {
			total += w.weighting;
		}

		// select a random value between 0 and our total
		int random = new Random().nextInt(total);

		// loop thru our weightings until we arrive at the correct one
		int current = 0;
		for (tWeighting w : weightingOptions) {
			current += w.weighting;
			if (random < current)
				return w.value;
		}
		// shouldn't happen.
		return -1;
	}

	public static void main(String[] args) {

		List<tWeighting> weightings = new ArrayList<tWeighting>();
		weightings.add(new tWeighting(0, 10));
		weightings.add(new tWeighting(1, 5));
		weightings.add(new tWeighting(2, 2));
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
