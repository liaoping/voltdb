/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import com.google_voltpatches.common.collect.ImmutableList;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AdHocNPPartitions implements JSONString {

    public final List<Integer> partitions;

    public AdHocNPPartitions(Collection<Integer> partitions)
    {
        this.partitions = ImmutableList.copyOf(partitions);
    }
    
    public AdHocNPPartitions(String partitionString)
    {
    	ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    	String[] partitionStrings = partitionString.split(",");
    	for (String part : partitionStrings) {
    		builder.add(Integer.valueOf(part));
    	}
    	this.partitions = builder.build();
    }


    public AdHocNPPartitions(JSONObject jsObj) throws JSONException
    {
    	partitions = parseRanges(jsObj);
    }

    private List<Integer> parseRanges(JSONObject jsObj) throws JSONException
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        JSONArray pairsArray = jsObj.getJSONArray("PartitionId");

        for (int i = 0; i < pairsArray.length(); i++) {
            JSONObject pairObj = pairsArray.getJSONObject(i);

            builder.add(pairObj.getInt("ID"));
        }

        return builder.build();
    }

    @Override
    public String toJSONString()
    {
        JSONStringer stringer = new JSONStringer();

        try {
            stringer.object();
            stringer.key("PartitionId").array();

            for (Integer id : partitions) {
                stringer.object();

                stringer.key("ID").value(id);

                stringer.endObject();
            }

            stringer.endArray();
            stringer.endObject();

            return stringer.toString();
        } catch (JSONException e) {
            return null;
        }
    }
}
