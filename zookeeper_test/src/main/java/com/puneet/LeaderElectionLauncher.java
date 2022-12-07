package com.puneet;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LeaderElectionLauncher {
	
	private static final Logger LOG = Logger.getLogger(LeaderElectionLauncher.class);
	
	public static void main(String[] args) throws IOException {

		final int id = new Random().nextInt();
		System.out.println("Random id is " + id);
		
		final ExecutorService service = Executors.newSingleThreadExecutor();

		final ProcessNode node = new ProcessNode(id, "localhost:2181");
		node.initialize();
		final Future<?> status = service.submit(node);
		
		try {
			status.get();
		} catch (InterruptedException | ExecutionException e) {
			LOG.fatal(e.getMessage(), e);
			service.shutdown();
		}
	}
}