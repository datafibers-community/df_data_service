package com.datafibers.flinknext;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import scala.concurrent.Await;
import scala.concurrent.Future;

import com.datafibers.model.DFJobPOPJ;

/**
 * For Unit Test Only
 * This is customized DF cluster client for TESTING communication with an Flink standalone (on-premise) cluster or an existing cluster that has been
 * brought up independently of a specific job. The runWithDFObj is added to pass DFPOPJ into job execution. The jobId is set to the jobConfig
 * immediately before submit Flink job.
 */
public class DFCusterClient extends ClusterClient {

    public DFCusterClient(Configuration config) throws Exception {
        super(config);
    }

    @Override
    public void waitForClusterToBeReady() {}

    @SuppressWarnings( "deprecation" )
    @Override
    public String getWebInterfaceURL() {
        String host = this.getJobManagerAddress().getHostString();
        int port = getFlinkConfiguration().getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
                ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
        return "http://" +  host + ":" + port;
    }

    @Override
    public GetClusterStatusResponse getClusterStatus() {
        ActorGateway jmGateway;
        try {
            jmGateway = getJobManagerGateway();
            Future<Object> future = jmGateway.ask(GetClusterStatus.getInstance(), timeout);
            Object result = Await.result(future, timeout);
            if (result instanceof GetClusterStatusResponse) {
                return (GetClusterStatusResponse) result;
            } else {
                throw new RuntimeException("Received the wrong reply " + result + " from cluster.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Couldn't retrieve the Cluster status.", e);
        }
    }

    @Override
    public List<String> getNewMessages() {
        return Collections.emptyList();
    }

    @Override
    public String getClusterIdentifier() {
        // Avoid blocking here by getting the address from the config without resolving the address
        return "DF Standalone cluster with JobManager at " + this.getJobManagerAddress();
    }

    @Override
    public int getMaxSlots() {
        return -1;
    }

    @Override
    protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader)
            throws ProgramInvocationException {
        if (isDetached()) {
            return super.runDetached(jobGraph, classLoader);
        } else {
            return super.run(jobGraph, classLoader);
        }
    }

    @Override
    protected void finalizeCluster() {}


    public JobSubmissionResult runWithDFObj(
            FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths, ClassLoader classLoader, DFJobPOPJ dfJobPOPJ) throws ProgramInvocationException {
        return runWithDFObj(compiledPlan, libraries, classpaths, classLoader, SavepointRestoreSettings.none(), dfJobPOPJ);
    }

    public JobSubmissionResult runWithDFObj(FlinkPlan compiledPlan,
            List<URL> libraries, List<URL> classpaths, ClassLoader classLoader, SavepointRestoreSettings savepointSettings, DFJobPOPJ dfJobPOPJ)
		throws ProgramInvocationException {
		JobGraph job = getJobGraph(compiledPlan, libraries, classpaths, savepointSettings);
		// Keep the jobID to DFPOPJ
		dfJobPOPJ.setFlinkIDToJobConfig(job.getJobID().toString());
		return submitJob(job, classLoader);
		}


	private JobGraph getJobGraph(FlinkPlan optPlan, List<URL> jarFiles, List<URL> classpaths, SavepointRestoreSettings savepointSettings) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
			job.setSavepointRestoreSettings(savepointSettings);
		} else {
			JobGraphGenerator gen = new JobGraphGenerator(this.flinkConfig);
			job = gen.compileJobGraph((OptimizedPlan) optPlan);
		}

		for (URL jar : jarFiles) {
			try {
				job.addJar(new Path(jar.toURI()));
			} catch (URISyntaxException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}
 
		job.setClasspaths(classpaths);

		return job;
	}


	@Override
	public boolean hasUserJarsInClassPath(List<URL> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

}
