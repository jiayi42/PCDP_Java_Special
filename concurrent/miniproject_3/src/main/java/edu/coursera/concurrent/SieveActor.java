package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import java.util.ArrayList;

import java.util.List;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static edu.rice.pcdp.PCDP.finish;


/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        SieveActorActor[] sieveActor = new SieveActorActor[1];
        List<Integer> result = new ArrayList<>();

        for (int i = 3; i <= limit; i += 2) {
            result.add(i);
        }

        finish(() -> {
            sieveActor[0] = new SieveActorActor(2);
            //do not parallel result as this actor needs to create and start next actor
            result.forEach( sieveActor[0]::send);
        });

        int localPrimesSize = 0;
        SieveActorActor currentActor = sieveActor[0];

        while(currentActor != null){
            currentActor = currentActor.nextActor;
            localPrimesSize += 1;
        }

        return localPrimesSize;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        int primeNum;
        SieveActorActor nextActor;

        SieveActorActor(int primeNum){
            this.primeNum = primeNum;
        };

        @Override
        public void process(final Object msg) {
            // casting
            final int candidate = (Integer) msg;

            if(candidate%primeNum == 0){
                return;
            }

            if(nextActor != null){
                nextActor.send(msg);
            }
            else{
                nextActor = new SieveActorActor(candidate);
            }

        }

    }
}
