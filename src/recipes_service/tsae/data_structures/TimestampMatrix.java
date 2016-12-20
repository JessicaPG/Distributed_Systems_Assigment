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
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{	
	
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	//default constructor
	public TimestampMatrix() {}

	public void setTimestampMatrix(ConcurrentHashMap<String, TimestampVector> timestampMatrix) {
		this.timestampMatrix = timestampMatrix;
	}

	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	public TimestampVector getTimestampVector(String node){
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		Set<String> hosts = tsMatrix.timestampMatrix.keySet();
		for (String host : hosts) {
			TimestampVector tsv = timestampMatrix.get(host);
			TimestampVector tref = tsMatrix.getTimestampVector(host);
			if(tsv != null)
				tsv.updateMax(tref);
		}	
		
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector){
		if(timestampMatrix.get(node) != null)
			timestampMatrix.replace(node, tsVector);
		else
			timestampMatrix.put(node, tsVector);
	}
	
	/**
	 * Obtain a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 * 
	 * @return minVector
	 */
	
	public synchronized TimestampVector minTimestampVector(){
		TimestampVector minVector = null;
		Set<String> hosts = timestampMatrix.keySet();

		//iterate between the vectors on this timestampMatrix
		for (String host : hosts) {
			TimestampVector tsv = timestampMatrix.get(host);
			if (minVector != null){
				//merge current vector (tsv) with minVector to 
				//obtain the minium timestamp of both of them
				minVector.mergeMin(tsv);
			}else{
				//Initialization of the minVector with a clone of
				//the current vector(tsv)
				minVector = tsv.clone();
			}
		}
		return minVector;
	}
	
	/**
	 * Clone a timestampMatrix
	 * @return copyTMatrix 
	 */
	public TimestampMatrix clone(){
		Set<String> hosts = timestampMatrix.keySet();
		ConcurrentHashMap<String, TimestampVector> copy = new ConcurrentHashMap<String, TimestampVector>();
		
		for (String host : hosts) {
			TimestampVector copyVector = timestampMatrix.get(host).clone();
			copy.put(host, copyVector);
		}
		
		List<String> hostsList = new ArrayList<String> (hosts);
		TimestampMatrix tsMat = new TimestampMatrix(hostsList);
		tsMat.setTimestampMatrix(copy);
		TimestampMatrix copyTMatrix = new TimestampMatrix();

		for(String host: hosts) {
			TimestampVector copyVector = timestampMatrix.get(host).clone();
			copyTMatrix.timestampMatrix.put(host, copyVector);
		}

		return copyTMatrix;
	}
	
	/**
	 * Equals
	 */
	public boolean equals(Object matrix) {
        if (matrix == null) 
            return false;
        if (this == matrix) 
            return true;
        if ((getClass() != matrix.getClass())) 
            return false;
  
        TimestampMatrix other = (TimestampMatrix) matrix;

        if (this.timestampMatrix == other.timestampMatrix) 
            return true;
        if (this.timestampMatrix == null) 
        	return false;
        if (other.timestampMatrix == null) 
            return false;
        else 
            return this.timestampMatrix.equals(other.timestampMatrix);
        
    }

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
