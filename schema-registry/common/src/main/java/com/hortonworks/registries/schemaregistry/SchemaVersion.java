/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifeCycleStates;

import java.io.Serializable;

/**
 * This class represents details about versioned instance of a schema which includes description and schema text.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaVersion implements Serializable {
    private static final long serialVersionUID = 1664618495690787804L;

    private String description;
    private String schemaText;
    private Byte initialState = SchemaVersionLifeCycleStates.ENABLED.id();

    @SuppressWarnings("unused")
    private SchemaVersion() { /* Private constructor for Jackson JSON mapping */ }

    public SchemaVersion(String schemaText, String description) {
        this.description = description;
        this.schemaText = schemaText;
    }

    public SchemaVersion(String schemaText, String description, Byte initialState) {
        this.description = description;
        this.schemaText = schemaText;
        if(initialState != null) {
            this.initialState = initialState;
        }
    }

    public String getDescription() {
        return description;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public Byte getInitialState() {
        return initialState;
    }

    @Override
    public String toString() {
        return "SchemaVersion{" +
                "description='" + description + '\'' +
                ", schemaText='" + schemaText + '\'' +
                ", initialState=" + initialState +
                '}';
    }
}
