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

import java.util.List;

/**
 *
 */
public class SchemaLifeCycleStateMachine {

    private SchemaLifeCycleStateMachine(Builder builder) {
    }

    public static SchemaLifeCycleStateMachine.Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private List<SchemaVersionLifeCycleState> states;

        public Builder withStates(List<SchemaVersionLifeCycleState> states) {
            this.states = states;
            return this;
        }

        public Builder withInitialState(SchemaVersionLifeCycleState state) {
            return this;
        }

        public Builder withTransition(SchemaVersionLifeCycleState fromState, Action action, List<SchemaVersionLifeCycleState> toStates) {
            // build graph
            return this;
        }

        public SchemaLifeCycleStateMachine build() {
            return new SchemaLifeCycleStateMachine(this);
        }

    }

    /**
     * Action to be executed from one state to another state. {@link #execute(SchemaVersionLifeCycleContext)} method should set
     * next state on schemaLifeCycleContext.
     */
    public interface Action {

        /**
         * Executes the given {@code schemaVersionLifeCycleContext} and sets the next state using {@link SchemaVersionLifeCycleContext#setState(SchemaVersionLifeCycleState)}
         * @param schemaVersionLifeCycleContext
         * @throws SchemaLifeCycleException
         */
        public void execute(SchemaVersionLifeCycleContext schemaVersionLifeCycleContext) throws SchemaLifeCycleException;
    }
}
