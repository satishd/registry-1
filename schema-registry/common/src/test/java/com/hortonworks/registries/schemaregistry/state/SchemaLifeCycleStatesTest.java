/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.state;

import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 *
 */
public class SchemaLifeCycleStatesTest {
    private static Logger LOG = LoggerFactory.getLogger(SchemaLifeCycleStatesTest.class);

    @Rule
    public TestName testName = new TestName();

    private SchemaLifeCycleContext context;

    @Before
    public void setup() {
        SchemaVersionService schemaVersionService = new SchemaVersionService() {
            @Override
            public void updateSchemaVersionState(SchemaLifeCycleContext schemaLifeCycleContext) {
                LOG.info("Updating schema version: [{}]", schemaLifeCycleContext);
            }

            @Override
            public void deleteSchemaVersion(Long schemaVersionId) {
                LOG.info("Deleting schema version [{}]", schemaVersionId);
            }

            @Override
            public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException {
                LOG.info("Archiving schema version [{}]", schemaVersionId);
            }
        };
        context = new SchemaLifeCycleContext(1L, schemaVersionService);
    }

    @Test
    public void testInitiatedState() throws Exception {

        SchemaLifeCycleState initiated = SchemaLifeCycleStates.INITIATED;

        DefaultSchemaReviewExecutor schemaReviewExecutor = createDefaultSchemaReviewExecutor();
        initiated.startReview(context, schemaReviewExecutor);
        initiated.enable(context);

        checkDisableNotSupported(initiated, context);
        checkArchiveNotSupported(initiated, context);
        checkDeleteNotSupported(initiated, context);
    }

    private DefaultSchemaReviewExecutor createDefaultSchemaReviewExecutor() {
        DefaultSchemaReviewExecutor schemaReviewExecutor = new DefaultSchemaReviewExecutor();
        schemaReviewExecutor.init(SchemaLifeCycleStates.REVIEWED, SchemaLifeCycleStates.CHANGES_REQUIRED, Collections.emptyMap());
        return schemaReviewExecutor;
    }

    @Test
    public void testEnabledState() throws Exception {

        SchemaLifeCycleState enabled = SchemaLifeCycleStates.ENABLED;

        enabled.disable(context);
        enabled.archive(context);

        checkStartReviewNotSupported(enabled, context);
        checkEnableNotSupported(enabled, context);
        checkDeleteNotSupported(enabled, context);
    }


    @Test
    public void testDisabledState() throws Exception {

        SchemaLifeCycleState disabled = SchemaLifeCycleStates.DISABLED;

        disabled.enable(context);
        disabled.archive(context);

        checkStartReviewNotSupported(disabled, context);
        checkDisableNotSupported(disabled, context);
        checkDeleteNotSupported(disabled, context);
    }

    @Test
    public void testArchivedState() throws Exception {

        SchemaLifeCycleState archived = SchemaLifeCycleStates.ARCHIVED;

        checkStartReviewNotSupported(archived, context);
        checkEnableNotSupported(archived, context);
        checkDisableNotSupported(archived, context);
        checkArchiveNotSupported(archived, context);
        checkDeleteNotSupported(archived, context);
    }

    @Test
    public void testDeletedState() throws Exception {

        SchemaLifeCycleState deleted = SchemaLifeCycleStates.DELETED;

        checkStartReviewNotSupported(deleted, context);
        checkEnableNotSupported(deleted, context);
        checkDisableNotSupported(deleted, context);
        checkArchiveNotSupported(deleted, context);
        checkDeleteNotSupported(deleted, context);
    }

    private void checkArchiveNotSupported(SchemaLifeCycleState state, SchemaLifeCycleContext context) {
        try {
            state.archive(context);
            Assert.fail(state.name() + " should not lead to archive state");
        } catch (SchemaLifeCycleException e) {
        }
    }


    private void checkDeleteNotSupported(SchemaLifeCycleState state, SchemaLifeCycleContext context) {
        try {
            state.delete(context);
            Assert.fail(state.name() + " should not lead to delete state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkDisableNotSupported(SchemaLifeCycleState state, SchemaLifeCycleContext context) {
        try {
            state.disable(context);
            Assert.fail(state.name() + " should not lead to disabled state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkEnableNotSupported(SchemaLifeCycleState state, SchemaLifeCycleContext context) {
        try {
            state.enable(context);
            Assert.fail(state.name() + " should not lead to enable state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkStartReviewNotSupported(SchemaLifeCycleState state, SchemaLifeCycleContext context) {
        try {
            state.startReview(context, createDefaultSchemaReviewExecutor());
            Assert.fail(state.name() + " should not lead to startReview state");
        } catch (SchemaLifeCycleException e) {
        }
    }
}
