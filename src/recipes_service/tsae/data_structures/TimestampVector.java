/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{	

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();

	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}
	
	/**
	 * Get concurrenthashmap
	 * @return hashMap
	 */
	public ConcurrentHashMap<String, Timestamp> getTimestampVector() {
		return timestampVector;
	}
	
	/**
	 * Set concurrenthashmap
	 * @param timestampVector
	 */
	public void setTimestampVector(ConcurrentHashMap<String, Timestamp> timestampVector) {
		this.timestampVector = timestampVector;
	}


	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		String host= timestamp.getHostid();
		
		if (timestamp != null)
			timestampVector.replace(host, timestamp);
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		if( tsVector != null ){
			Set<String> hosts = this.timestampVector.keySet();
		
			
			//iterate over the keys (host) of the hashmap to update if it is necessary,
			//its timestamp to be maximum
			for (String host : hosts) {
				Timestamp max = tsVector.timestampVector.get(host);
				
				if( max != null ) {
					//replace if the timestampVector is higher than the current
					if ( this.timestampVector.get(host).compare(max) < 0 ) { 
						timestampVector.replace(host, max);
					}
				}
			}	
		}
	}
	

	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		if( tsVector != null ) {
			Set<String> hosts = tsVector.timestampVector.keySet();
			Timestamp min = null;
			
			//iterate over the keys (host) of the hashmap to update if it is necessary,
			//its timestamp to be minimum
			for (String host : hosts) {
				min = tsVector.timestampVector.get(host);

				if( this.timestampVector.get(host) != null ) {
					//replace if the timestampVector is lower than the current
					if ( min.compare(this.timestampVector.get(host)) < 0) {
						timestampVector.replace(host, min);
					}
				} else {
					timestampVector.put(host, min);
				}
			}
		}
	}
	
	/**
	 * clone TimestampVector
	 */
	public TimestampVector clone(){
		
		Set<String> hosts = timestampVector.keySet();
		ConcurrentHashMap<String, Timestamp> copy = new ConcurrentHashMap<String, Timestamp>();
		
		for (String host : hosts) {
			copy.put(host, timestampVector.get(host));
		}
		
		List<String> hostsList = new ArrayList<String> (hosts);
		TimestampVector tsVec = new TimestampVector(hostsList);
		tsVec.setTimestampVector(copy);
		
		return tsVec;
	}
	
	/**
	 * equals
	 */
	public boolean equals(Object tsVector){
		if (tsVector == null)
			return false;
		if(this == tsVector)
			return true;
		if (getClass() != tsVector.getClass())
			return false;
		
		TimestampVector other = (TimestampVector) tsVector;

		if (this.timestampVector == other.timestampVector)
			return true;
		if(this.timestampVector == null)
			return false;
		if(other.timestampVector == null)
			return false;
        else 
            return this.timestampVector.equals(other.timestampVector);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}

	public Timestamp getLast(String node1) {
		return this.timestampVector.get(node1);
		
	}
}
