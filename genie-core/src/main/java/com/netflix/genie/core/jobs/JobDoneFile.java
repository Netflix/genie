package com.netflix.genie.core.jobs;

/**
 * Class that represents the structure of the genie.done file created when a job is done.
 *
 * @author amsharma
 */
public class JobDoneFile {
    private int exitCode;

    public int getExitCode() {
        return exitCode;
    }

    public void setExitCode(
        final int exitCode
    ) {
        this.exitCode = exitCode;
    }
}
