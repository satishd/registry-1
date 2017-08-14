/*
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
 */
package com.hortonworks.registries.schemaregistry.state;

import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class SchemaLifeCycleContext {
    private SchemaLifeCycleState state;
    private Long schemaVersionId;
    private Integer sequence;
    private SchemaVersionService schemaVersionService;
    private ConcurrentMap<String, Object> props = new ConcurrentHashMap<>();

    public SchemaLifeCycleContext(Long schemaVersionId,
                                  Integer sequence,
                                  SchemaVersionService schemaVersionService) {
        this.schemaVersionId = schemaVersionId;
        this.sequence = sequence;
        this.schemaVersionService = schemaVersionService;
    }

    public void setState(SchemaLifeCycleState state) {
        this.state = state;
    }

    public SchemaLifeCycleState getState() {
        return state;
    }

    public Long getSchemaVersionId() {
        return schemaVersionId;
    }

    public Integer getSequence() {
        return sequence;
    }

    public SchemaVersionService getSchemaVersionService() {
        return schemaVersionService;
    }

    public Object getProperty(String key) {
        return props.get(key);
    }

    public Object setProperty(String key, String value) {
        return props.put(key, value);
    }

    public void updateSchemaVersionState() throws SchemaLifeCycleException, SchemaNotFoundException {
        schemaVersionService.updateSchemaVersionState(this);
    }

    @Override
    public String toString() {
        return "SchemaLifeCycleContext{" +
                "state=" + state +
                ", schemaVersionId=" + schemaVersionId +
                ", schemaVersionService=" + schemaVersionService +
                ", props=" + props +
                '}';
    }
}
