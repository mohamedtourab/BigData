package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 25 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	HashSet<String> users;

    	// Each user in values is potential friend of the other users in values
    	// because they have the user "key" in common.
    	// Hence, the users in values are potential friends of each others.    	
    	// Since it is not possible to iterate more than one time on values
    	// we need to create a local copy of it. However, the 
    	// size of values is at most equal to the friend of user "key". Hence,
    	// it is a small list

    	users=new HashSet<String>();

        for (Text value : values) {
        	users.add(value.toString());
        }
    	
    	
        // Compute the list of potential friends for each user in users
    
        for (String currentUser: users)
        {
        	String listOfPotentialFriends=new String("");
    	
        	for (String potFriend: users) 
        	{	// If potFriend is not currentUser then include him/her in the 
        		// potential friends of currentUser
        		if (currentUser.compareTo(potFriend)!=0)
        			listOfPotentialFriends=listOfPotentialFriends.concat(potFriend+" ");
        	}
        	
        	// Check if currentUser has at least one friend
        	if (listOfPotentialFriends.compareTo("")!=0)
        		context.write(new Text(currentUser), new Text(listOfPotentialFriends));
        
        }
    }
}
