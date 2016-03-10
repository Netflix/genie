/*
 *
 *  Copyright 2015 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit Tests for Sequence Workflow executor.
 *
 * @author amsharma
 */
@Category(UnitTest.class)
public class SequenceWorkFlowExecutorUnitTests {

    private WorkflowTask workflowTask;
    private SequenceWorkflowExecutor sequenceWorkflowExecutor;

    /**
     * Test the execute workflow method when workflow fails.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testExecuteWorkflowFailure() throws GenieException {
        workflowTask = Mockito.mock(ApplicationTask.class);
        final List<WorkflowTask> workflowTasks = new ArrayList<>();
        workflowTasks.add(workflowTask);
        final Map<String, Object> context = new HashMap<>();
        Mockito.doThrow(GenieException.class).when(this.workflowTask).executeTask(Mockito.eq(context));

        sequenceWorkflowExecutor = new SequenceWorkflowExecutor();
        Assert.assertFalse(sequenceWorkflowExecutor.executeWorkflow(workflowTasks, context));
    }

    /**
     * Test the execute workflow method when workflow succeeds.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testExecuteWorkflowSuccess() throws GenieException {
        workflowTask = Mockito.mock(ApplicationTask.class);
        final List<WorkflowTask> workflowTasks = new ArrayList<>();
        workflowTasks.add(workflowTask);
        final Map<String, Object> context = new HashMap<>();

        sequenceWorkflowExecutor = new SequenceWorkflowExecutor();
        Assert.assertTrue(sequenceWorkflowExecutor.executeWorkflow(workflowTasks, context));
    }
}
