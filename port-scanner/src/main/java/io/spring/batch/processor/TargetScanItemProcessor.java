package io.spring.batch.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import io.spring.batch.domain.Target;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.item.ItemProcessor;

/**
 * Takes a {@link Target} instance and scans it, recording if the port is open
 * and any banner information that is returned.
 * 
 * @author Michael Minella
 */
public class TargetScanItemProcessor implements ItemProcessor<Target, Target> {

	protected static final Log logger = LogFactory.getLog(TargetScanItemProcessor.class);

	@Override
	public Target process(Target curTarget) throws Exception {
		BufferedReader input = null;
		Socket socket = null;
		try {
			System.out.println("About to check: " + curTarget.getIp() + ":" + curTarget.getPort() + " on thread " + Thread.currentThread().getName());

			socket = new Socket();
			socket.connect(new InetSocketAddress(curTarget.getIp(), curTarget.getPort()), 1000);
			socket.setSoTimeout(1000);
			curTarget.setConnected(true);
			input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			String banner = "";
			curTarget.setBanner(banner);

			while ((banner = input.readLine()) != null) {
				logger.debug("curLine = |" + banner + "|");
				curTarget.setBanner(curTarget.getBanner() + banner);
			}

			if(curTarget.getBanner().length() <= 0) {
				curTarget.setBanner(null);
			}
		} catch (SocketTimeoutException ignore) {
		} catch (ConnectException ignore) {
			System.out.println("The connection was refused on port " + curTarget.getPort());
		} catch (Throwable error) {
			System.out.println("An exception was thrown: " + error.getClass() + " with the message " + error.getMessage());
		} finally {
			if(input != null) {
				input.close();
			}

			if(socket != null) {
				socket.close();
			}
		}

		return curTarget;
	}
}