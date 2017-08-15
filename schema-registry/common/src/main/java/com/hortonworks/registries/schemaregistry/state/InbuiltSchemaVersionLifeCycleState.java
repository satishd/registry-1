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

import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.List;

/**
 *
 */
public interface InbuiltSchemaVersionLifeCycleState extends SchemaVersionLifeCycleState {

    /**
     * @return List of states that can lead from this state.
     */
    public List<SchemaVersionLifeCycleState> nextStates();

    public default void startReview(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext,
                                    SchemaReviewExecutor schemaReviewExecutor) throws SchemaLifeCycleException, SchemaNotFoundException {
        throw new SchemaLifeCycleException(" This operation is not supported for this instance: " + this);
    }

    public default void enable(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext) throws SchemaLifeCycleException, SchemaNotFoundException, IncompatibleSchemaException {
        throw new SchemaLifeCycleException(" This operation is not supported for this instance: " + this);
    }

    public default void disable(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext) throws SchemaLifeCycleException, SchemaNotFoundException {
        throw new SchemaLifeCycleException(" This operation is not supported for this instance: " + this);

    }

    public default void archive(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext) throws SchemaLifeCycleException, SchemaNotFoundException {
        throw new SchemaLifeCycleException(" This operation is not supported for this instance: " + this);
    }

    public default void delete(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext) throws SchemaLifeCycleException, SchemaNotFoundException {
        throw new SchemaLifeCycleException(" This operation is not supported for this instance: " + this);
    }


}
