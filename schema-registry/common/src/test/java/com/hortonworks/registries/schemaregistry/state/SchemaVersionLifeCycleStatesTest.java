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

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;

/**
 *
 */
@Ignore
public class SchemaVersionLifeCycleStatesTest {
    private static Logger LOG = LoggerFactory.getLogger(SchemaVersionLifeCycleStatesTest.class);

    @Rule
    public TestName testName = new TestName();

    private SchemaVersionLifeCycleContext context;

    @Before
    public void setup() {
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("schema-1").type("avro")
                                                                              .schemaGroup("kafka")
                                                                              .build();
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata, new Random().nextLong(), System.currentTimeMillis());
        SchemaVersionInfo schemaVersionInfo =
                new SchemaVersionInfo(new Random().nextLong(), schemaMetadata.getName(), 1, schemaMetadataInfo.getId(),
                                      "{\"type\":\"string\"}", System.currentTimeMillis(),
                                      "", SchemaVersionLifeCycleStates.ENABLED.id());

        SchemaVersionService schemaVersionServiceMock = new SchemaVersionService() {
            @Override
            public void updateSchemaVersionState(SchemaVersionLifeCycleContext schemaLifeCycleContext) {
                LOG.info("Updating schema version: [{}]", schemaLifeCycleContext);
            }

            @Override
            public void deleteSchemaVersion(Long schemaVersionId) {
                LOG.info("Deleting schema version [{}]", schemaVersionId);
            }

            @Override
            public SchemaMetadataInfo getSchemaMetadata(long schemaVersionId) throws SchemaNotFoundException {
                return schemaMetadataInfo;
            }

            @Override
            public SchemaVersionInfo getSchemaVersionInfo(long schemaVersionId) throws SchemaNotFoundException {
                return schemaVersionInfo;
            }

            @Override
            public CompatibilityResult checkForCompatibility(SchemaMetadata schemaMetadata,
                                                             String toSchemaText,
                                                             String existingSchemaText) {
                return CompatibilityResult.SUCCESS;
            }

            @Override
            public Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaName) throws SchemaNotFoundException {
                return Collections.singletonList(schemaVersionInfo);
            }

        };
        context = new SchemaVersionLifeCycleContext(schemaVersionInfo.getId(), 1, schemaVersionServiceMock, new SchemaVersionLifeCycleStates.Registry());
    }

    @Test
    public void testInitiatedState() throws Exception {

        InbuiltSchemaVersionLifeCycleState initiated = SchemaVersionLifeCycleStates.INITIATED;

        DefaultSchemaReviewExecutor schemaReviewExecutor = createDefaultSchemaReviewExecutor();
        initiated.startReview(context, schemaReviewExecutor);
        initiated.enable(context);

        checkDisableNotSupported(initiated, context);
        checkArchiveNotSupported(initiated, context);
        checkDeleteNotSupported(initiated, context);
    }

    private DefaultSchemaReviewExecutor createDefaultSchemaReviewExecutor() {
        DefaultSchemaReviewExecutor schemaReviewExecutor = new DefaultSchemaReviewExecutor();
        schemaReviewExecutor.init(SchemaVersionLifeCycleStates.REVIEWED, SchemaVersionLifeCycleStates.CHANGES_REQUIRED, Collections.emptyMap());
        return schemaReviewExecutor;
    }

    @Test
    public void testEnabledState() throws Exception {

        InbuiltSchemaVersionLifeCycleState enabled = SchemaVersionLifeCycleStates.ENABLED;

        enabled.disable(context);
        enabled.archive(context);

        checkStartReviewNotSupported(enabled, context);
        checkEnableNotSupported(enabled, context);
        checkDeleteNotSupported(enabled, context);
    }


    @Test
    public void testDisabledState() throws Exception {

        InbuiltSchemaVersionLifeCycleState disabled = SchemaVersionLifeCycleStates.DISABLED;

        disabled.enable(context);
        disabled.archive(context);

        checkStartReviewNotSupported(disabled, context);
        checkDisableNotSupported(disabled, context);
        checkDeleteNotSupported(disabled, context);
    }

    @Test
    public void testArchivedState() throws Exception {

        InbuiltSchemaVersionLifeCycleState archived = SchemaVersionLifeCycleStates.ARCHIVED;

        checkStartReviewNotSupported(archived, context);
        checkEnableNotSupported(archived, context);
        checkDisableNotSupported(archived, context);
        checkArchiveNotSupported(archived, context);
        checkDeleteNotSupported(archived, context);
    }

    @Test
    public void testDeletedState() throws Exception {

        InbuiltSchemaVersionLifeCycleState deleted = SchemaVersionLifeCycleStates.DELETED;

        checkStartReviewNotSupported(deleted, context);
        checkEnableNotSupported(deleted, context);
        checkDisableNotSupported(deleted, context);
        checkArchiveNotSupported(deleted, context);
        checkDeleteNotSupported(deleted, context);
    }

    private void checkArchiveNotSupported(InbuiltSchemaVersionLifeCycleState state,
                                          SchemaVersionLifeCycleContext context) throws SchemaNotFoundException {
        try {
            state.archive(context);
            Assert.fail(state.name() + " should not lead to archive state");
        } catch (SchemaLifeCycleException e) {
        }
    }


    private void checkDeleteNotSupported(InbuiltSchemaVersionLifeCycleState state,
                                         SchemaVersionLifeCycleContext context) throws SchemaNotFoundException {
        try {
            state.delete(context);
            Assert.fail(state.name() + " should not lead to delete state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkDisableNotSupported(InbuiltSchemaVersionLifeCycleState state,
                                          SchemaVersionLifeCycleContext context) throws SchemaNotFoundException {
        try {
            state.disable(context);
            Assert.fail(state.name() + " should not lead to disabled state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkEnableNotSupported(InbuiltSchemaVersionLifeCycleState state,
                                         SchemaVersionLifeCycleContext context) throws SchemaNotFoundException, IncompatibleSchemaException {
        try {
            state.enable(context);
            Assert.fail(state.name() + " should not lead to enable state");
        } catch (SchemaLifeCycleException e) {
        }
    }

    private void checkStartReviewNotSupported(InbuiltSchemaVersionLifeCycleState state,
                                              SchemaVersionLifeCycleContext context) throws SchemaNotFoundException {
        try {
            state.startReview(context, createDefaultSchemaReviewExecutor());
            Assert.fail(state.name() + " should not lead to startReview state");
        } catch (SchemaLifeCycleException e) {
        }
    }
}
