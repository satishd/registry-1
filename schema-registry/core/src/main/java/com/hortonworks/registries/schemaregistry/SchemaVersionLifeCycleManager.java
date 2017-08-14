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
package com.hortonworks.registries.schemaregistry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.SlotSynchronizer;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifeCycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaLifeCycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifeCycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaLifeCycleStates;
import com.hortonworks.registries.schemaregistry.state.SchemaReviewExecutor;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionService;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.exception.StorageException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class SchemaVersionLifeCycleManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionLifeCycleManager.class);

    private static final String DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS = "com.hortonworks.registries.schemaregistry.state.DefaultSchemaReviewExecutor";
    public static final SchemaLifeCycleState DEFAULT_VERSION_STATE = SchemaLifeCycleStates.INITIATED;
    private SchemaReviewExecutor schemaReviewExecutor;
    private SchemaVersionInfoCache schemaVersionInfoCache;
    private SchemaVersionRetriever schemaVersionRetriever;
    private SlotSynchronizer<String> slotSynchronizer = new SlotSynchronizer<>();
    private static final int DEFAULT_RETRY_CT = 5;
    private StorageManager storageManager;
    private DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher;

    // todo remove this lock usage
    private Object addOrUpdateLock = new Object();

    public SchemaVersionLifeCycleManager(StorageManager storageManager,
                                         Map<String, Object> props,
                                         DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher) {
        this.storageManager = storageManager;
        this.schemaMetadataFetcher = schemaMetadataFetcher;

        Options options = new Options(props);
        schemaVersionRetriever = createSchemaVersionRetriever();

        schemaVersionInfoCache = new SchemaVersionInfoCache(
                schemaVersionRetriever,
                options.getMaxSchemaCacheSize(),
                options.getSchemaExpiryInSecs());

        schemaReviewExecutor = createSchemaReviewExecutor(props);
    }

    private SchemaReviewExecutor createSchemaReviewExecutor(Map<String, Object> props) {
        Map<String, Object> schemaReviewExecConfig = (Map<String, Object>) props.getOrDefault("schemaReviewExecutor",
                                                                                              Collections.emptyMap());
        String className = (String) schemaReviewExecConfig.getOrDefault("className", DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS);
        Map<String, ?> executorProps = (Map<String, ?>) schemaReviewExecConfig.getOrDefault("props", Collections.emptyMap());
        SchemaReviewExecutor schemaReviewExecutor;
        try {
            schemaReviewExecutor = (SchemaReviewExecutor) Class.forName(className,
                                                                        true,
                                                                        Thread.currentThread().getContextClassLoader())
                                                               .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error encountered while loading SchemaReviewExecutor [{}]", className, e);
            throw new IllegalArgumentException(e);
        }
        schemaReviewExecutor.init(SchemaLifeCycleStates.REVIEWED, SchemaLifeCycleStates.CHANGES_REQUIRED, executorProps);

        return schemaReviewExecutor;
    }

    public SchemaVersionRetriever getSchemaVersionRetriever() {
        return schemaVersionRetriever;
    }

    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion,
                                            Function<SchemaMetadata, Long> registerSchemaMetadataFn)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo;
        String schemaName = schemaMetadata.getName();
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo retrievedschemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        Long schemaMetadataId;
        if (retrievedschemaMetadataInfo != null) {
            schemaMetadataId = retrievedschemaMetadataInfo.getId();
            // check whether the same schema text exists
            schemaVersionInfo = getSchemaVersionInfo(schemaName, schemaVersion.getSchemaText());
            if (schemaVersionInfo == null) {
                schemaVersionInfo = createSchemaVersion(schemaMetadata,
                                                        retrievedschemaMetadataInfo.getId(),
                                                        schemaVersion.getSchemaText(),
                                                        schemaVersion.getDescription());

            }
        } else {
            schemaMetadataId = registerSchemaMetadataFn.apply(schemaMetadata);
            schemaVersionInfo = createSchemaVersion(schemaMetadata,
                                                    schemaMetadataId,
                                                    schemaVersion.getSchemaText(),
                                                    schemaVersion.getDescription());
        }

        return new SchemaIdVersion(schemaMetadataId, schemaVersionInfo.getVersion(), schemaVersionInfo.getId());
    }

    public SchemaIdVersion addSchemaVersion(String schemaName,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException {

        SchemaVersionInfo schemaVersionInfo;
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo != null) {
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            // check whether the same schema text exists
            schemaVersionInfo = findSchemaVersion(schemaMetadata.getType(), schemaVersion.getSchemaText(), schemaMetadataInfo
                    .getId());
            if (schemaVersionInfo == null) {
                schemaVersionInfo = createSchemaVersion(schemaMetadata,
                                                        schemaMetadataInfo.getId(),
                                                        schemaVersion.getSchemaText(),
                                                        schemaVersion.getDescription());
            }
        } else {
            throw new SchemaNotFoundException("Schema not found with the given schemaName: " + schemaName);
        }

        return new SchemaIdVersion(schemaMetadataInfo.getId(), schemaVersionInfo.getVersion(), schemaVersionInfo.getId());
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);

        SchemaVersionInfo latestSchema = null;
        if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                if (schemaVersionInfo.getVersion() > curVersion) {
                    latestSchema = schemaVersionInfo;
                    curVersion = schemaVersionInfo.getVersion();
                }
            }
        }

        return latestSchema;
    }

    private SchemaVersionInfo createSchemaVersion(SchemaMetadata schemaMetadata,
                                                  Long schemaMetadataId,
                                                  String schemaText,
                                                  String description)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        String type = schemaMetadata.getType();
        if (getSchemaProvider(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        // generate fingerprint, it parses the schema and checks for semantic validation.
        // throws InvalidSchemaException for invalid schemas.
        final String fingerprint = getFingerprint(type, schemaText);
        final String schemaName = schemaMetadata.getName();

        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        final Long schemaVersionStorableId = storageManager.nextId(schemaVersionStorable.getNameSpace());
        schemaVersionStorable.setId(schemaVersionStorableId);
        schemaVersionStorable.setSchemaMetadataId(schemaMetadataId);

        schemaVersionStorable.setFingerprint(fingerprint);

        schemaVersionStorable.setName(schemaName);

        schemaVersionStorable.setSchemaText(schemaText);
        schemaVersionStorable.setDescription(description);
        schemaVersionStorable.setTimestamp(System.currentTimeMillis());

        schemaVersionStorable.setState(DEFAULT_VERSION_STATE.id());

        // take a lock for a schema with same name.
        SlotSynchronizer.Lock slotLock = slotSynchronizer.lockSlot(schemaName);
        try {
            int retryCt = 0;
            while (true) {
                try {
                    Integer version = 0;
                    if (schemaMetadata.isEvolve()) {
                        CompatibilityResult compatibilityResult = checkCompatibility(schemaName, schemaText);
                        if (!compatibilityResult.isCompatible()) {
                            String errMsg = String.format("Given schema is not compatible with latest schema versions. \n" +
                                                                  "Error location: [%s] \n" +
                                                                  "Error encountered is: [%s]",
                                                          compatibilityResult.getErrorLocation(),
                                                          compatibilityResult.getErrorMessage());
                            LOG.error(errMsg);
                            throw new IncompatibleSchemaException(errMsg);
                        }
                        SchemaVersionInfo latestSchemaVersionInfo = getLatestSchemaVersionInfo(schemaName);
                        if (latestSchemaVersionInfo != null) {
                            version = latestSchemaVersionInfo.getVersion();
                        }
                    }
                    schemaVersionStorable.setVersion(version + 1);

                    storageManager.add(schemaVersionStorable);
                    try {
                        DEFAULT_VERSION_STATE.enable(new SchemaLifeCycleContext(schemaVersionStorableId, 1, createSchemaVersionService()));
                    } catch (SchemaLifeCycleException e) {
                        throw new RuntimeException(e);
                    }

                    break;
                } catch (StorageException e) {
                    // optimistic to try the next try would be successful. When retry attempts are exhausted, throw error back to invoker.
                    if (++retryCt == DEFAULT_RETRY_CT) {
                        LOG.error("Giving up after retry attempts [{}] while trying to add new version of schema with metadata [{}]", retryCt, schemaMetadata, e);
                        throw e;
                    }
                    LOG.debug("Encountered storage exception while trying to add a new version, attempting again : [{}] with error: [{}]", retryCt, e);
                }
            }

            // fetching this as the ID may have been set by storage manager.
            Long schemaInstanceId = schemaVersionStorable.getId();
            String storableNamespace = new SchemaFieldInfoStorable().getNameSpace();
            List<SchemaFieldInfo> schemaFieldInfos = getSchemaProvider(type).generateFields(schemaVersionStorable.getSchemaText());
            for (SchemaFieldInfo schemaFieldInfo : schemaFieldInfos) {
                final Long fieldInstanceId = storageManager.nextId(storableNamespace);
                SchemaFieldInfoStorable schemaFieldInfoStorable = SchemaFieldInfoStorable.fromSchemaFieldInfo(schemaFieldInfo, fieldInstanceId);
                schemaFieldInfoStorable.setSchemaInstanceId(schemaInstanceId);
                schemaFieldInfoStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(schemaFieldInfoStorable);
            }
        } finally {
            slotLock.unlock();
        }

        return schemaVersionStorable.toSchemaVersionInfo();
    }

    private SchemaProvider getSchemaProvider(String type) {
        return schemaMetadataFetcher.getSchemaProvider(type);
    }

    public CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        SchemaValidationLevel validationLevel = schemaMetadata.getValidationLevel();
        CompatibilityResult compatibilityResult;
        switch (validationLevel) {
            case LATEST:
                SchemaVersionInfo latestSchemaVersionInfo = getLatestSchemaVersionInfo(schemaName);
                compatibilityResult = checkCompatibility(schemaMetadata.getType(),
                                                         toSchema,
                                                         latestSchemaVersionInfo.getSchemaText(),
                                                         schemaMetadata.getCompatibility());
                if (!compatibilityResult.isCompatible()) {
                    LOG.info("Received schema is not compatible with the latest schema versions [{}] with schema name [{}]",
                             latestSchemaVersionInfo.getVersion(), schemaName);
                    return compatibilityResult;
                }
                break;
            case ALL:
                Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);
                for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                    compatibilityResult = checkCompatibility(schemaMetadata.getType(),
                                                             toSchema,
                                                             schemaVersionInfo.getSchemaText(),
                                                             schemaMetadata.getCompatibility());
                    if (!compatibilityResult.isCompatible()) {
                        LOG.info("Received schema is not compatible with one of the schema versions [{}] with schema name [{}]",
                                 schemaVersionInfo.getVersion(), schemaName);
                        return compatibilityResult;
                    }
                }
                break;
        }
        return CompatibilityResult.createCompatibleResult(toSchema);
    }

    private CompatibilityResult checkCompatibility(String type,
                                                   String toSchema,
                                                   String existingSchema,
                                                   SchemaCompatibility compatibility) {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.checkCompatibility(toSchema, existingSchema, compatibility);
    }

    public Collection<SchemaVersionInfo> getAllVersions(final String schemaName) throws SchemaNotFoundException {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.NAME, schemaName));

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema not found with name " + schemaName);
        }

        Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        List<SchemaVersionInfo> schemaVersionInfos;
        if (storables != null && !storables.isEmpty()) {
            schemaVersionInfos = storables
                    .stream()
                    .map(SchemaVersionStorable::toSchemaVersionInfo)
                    .collect(Collectors.toList());
        } else {
            schemaVersionInfos = Collections.emptyList();
        }
        return schemaVersionInfos;
    }

    private SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataFetcher.getSchemaMetadataInfo(schemaName);
    }

    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText) throws SchemaNotFoundException, InvalidSchemaException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaName);
        }

        Long schemaMetadataId = schemaMetadataInfo.getId();
        return findSchemaVersion(schemaMetadataInfo.getSchemaMetadata().getType(), schemaText, schemaMetadataId);
    }

    private SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(id));

        SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
        if (versionedSchema == null) {
            throw new SchemaNotFoundException("No Schema version exists with id " + id);
        }
        return versionedSchema.toSchemaVersionInfo();
    }

    private SchemaVersionInfo findSchemaVersion(String type,
                                                String schemaText,
                                                Long schemaMetadataId) throws InvalidSchemaException, SchemaNotFoundException {
        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                new QueryParam(SchemaVersionStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);

        SchemaVersionStorable schemaVersionStorable = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataId: [{}] and schemaText [{}]", schemaMetadataId, schemaText);
            }

            schemaVersionStorable = versionedSchemas.iterator().next();
        }

        return schemaVersionStorable == null ? null : schemaVersionStorable.toSchemaVersionInfo();
    }

    private String getFingerprint(String type,
                                  String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        return Hex.encodeHexString(schemaProvider.getFingerprint(schemaText));
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
    }

    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = new SchemaVersionInfoCache.Key(schemaVersionKey);
        SchemaVersionInfo schemaVersionInfo = schemaVersionInfoCache.getSchema(schemaVersionCacheKey);
        synchronized (addOrUpdateLock) {
            schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
            storageManager.remove(createSchemaVersionStorableKey(schemaVersionInfo.getId()));
        }
    }

    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifeCycleException {
        ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> pair = createSchemaLifeCycleContextAndState(schemaVersionId);
        pair.getRight().enable(pair.getLeft());
    }

    private ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> createSchemaLifeCycleContextAndState(Long schemaVersionId)
            throws SchemaNotFoundException {
        // get the current state from storage for the given versionID
        // we can use a query to get max value for the column for a given schema-version-id but StorageManager does not
        // have API to take custom queries.
        // todo we can have limit by to address this scenario
        Collection<SchemaVersionStateStorable> schemaVersionStates =
                storageManager.find(SchemaVersionStateStorable.NAME_SPACE,
                                    Collections.singletonList(new QueryParam(SchemaVersionStateStorable.SCHEMA_VERSION_ID,
                                                                             schemaVersionId.toString())),
                                    Collections.singletonList(OrderByField.of(SchemaVersionStateStorable.SEQUENCE)));
        if (schemaVersionStates.isEmpty()) {
            throw new SchemaNotFoundException("No schema versions found with id " + schemaVersionId);
        }
        SchemaVersionStateStorable stateStorable = schemaVersionStates.iterator().next();

        SchemaLifeCycleState schemaLifeCycleState = SchemaLifeCycleStates.Registry.getInstance()
                                                                                  .get(stateStorable.getStateId());
        SchemaVersionService schemaVersionService = createSchemaVersionService();
        SchemaLifeCycleContext context = new SchemaLifeCycleContext(stateStorable.getSchemaVersionId(),
                                                                    stateStorable.getSequence(),
                                                                    schemaVersionService);
        return new ImmutablePair<>(context, schemaLifeCycleState);
    }

    private SchemaVersionService createSchemaVersionService() {
        return new SchemaVersionService() {

            public void updateSchemaVersionState(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaNotFoundException {
                storeSchemaVersionState(schemaLifeCycleContext);
            }

            public void deleteSchemaVersion(Long schemaVersionId) {
                doDeleteSchemaVersion(schemaVersionId);
            }

            public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException {
            }
        };
    }

    private void storeSchemaVersionState(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaNotFoundException {
        synchronized (addOrUpdateLock) {
            // store versions state, sequence
            SchemaVersionStateStorable stateStorable = new SchemaVersionStateStorable();
            Long schemaVersionId = schemaLifeCycleContext.getSchemaVersionId();
            byte stateId = schemaLifeCycleContext.getState().id();

            stateStorable.setSchemaVersionId(schemaVersionId);
            stateStorable.setSequence(schemaLifeCycleContext.getSequence() + 1);
            stateStorable.setStateId(stateId);
            stateStorable.setTimestamp(System.currentTimeMillis());

            storageManager.add(stateStorable);

            // store latest state in versions entity
            StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(schemaVersionId));
            SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
            if (versionedSchema == null) {
                throw new SchemaNotFoundException("No Schema version exists with id " + schemaVersionId);
            }
            versionedSchema.setState(stateId);
            storageManager.addOrUpdate(versionedSchema);

            // invalidate schema version from cache
            SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
            schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
        }
    }

    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifeCycleException {
        ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> pair = createSchemaLifeCycleContextAndState(schemaVersionId);
        pair.getRight().delete(pair.getLeft());
    }

    private void doDeleteSchemaVersion(Long schemaVersionId) {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
        synchronized (addOrUpdateLock) {
            storageManager.remove(createSchemaVersionStorableKey(schemaVersionId));
            schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
        }
    }

    private StorableKey createSchemaVersionStorableKey(Long id) {
        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        schemaVersionStorable.setId(id);
        return schemaVersionStorable.getStorableKey();
    }

    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifeCycleException {
        ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> pair = createSchemaLifeCycleContextAndState(schemaVersionId);
        pair.getRight().archive(pair.getLeft());
    }


    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifeCycleException {
        ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> pair = createSchemaLifeCycleContextAndState(schemaVersionId);
        pair.getRight().disable(pair.getLeft());
    }

    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifeCycleException {
        ImmutablePair<SchemaLifeCycleContext, SchemaLifeCycleState> pair = createSchemaLifeCycleContextAndState(schemaVersionId);
        pair.getRight().startReview(pair.getLeft(), schemaReviewExecutor);
    }

    private SchemaVersionInfo retrieveSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();
        Integer version = schemaVersionKey.getVersion();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);

        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaName);
        }

        return fetchSchemaVersionInfo(version, schemaMetadataInfo);
    }

    private SchemaVersionInfo retrieveSchemaVersionInfo(SchemaIdVersion key) throws SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo = null;
        if (key.getSchemaVersionId() != null) {
            schemaVersionInfo = fetchSchemaVersionInfo(key.getSchemaVersionId());
        } else if (key.getSchemaMetadataId() != null) {
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(key.getSchemaMetadataId());
            Integer version = key.getVersion();

            schemaVersionInfo = fetchSchemaVersionInfo(version, schemaMetadataInfo);
        } else {
            throw new IllegalArgumentException("Invalid SchemaIdVersion: " + key);
        }

        return schemaVersionInfo;
    }

    private SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaMetadataFetcher.getSchemaMetadataInfo(schemaMetadataId);
    }

    private SchemaVersionRetriever createSchemaVersionRetriever() {
        return new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return retrieveSchemaVersionInfo(key);
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return retrieveSchemaVersionInfo(key);
            }
        };
    }


    private SchemaVersionInfo fetchSchemaVersionInfo(Integer version,
                                                     SchemaMetadataInfo schemaMetadataInfo) throws SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo = null;
        if (SchemaVersionKey.LATEST_VERSION.equals(version)) {
            schemaVersionInfo = getLatestSchemaVersionInfo(schemaMetadataInfo.getSchemaMetadata().getName());
        } else {
            Long schemaMetadataId = schemaMetadataInfo.getId();
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                    new QueryParam(SchemaVersionStorable.VERSION, version.toString()));

            Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                if (versionedSchemas.size() > 1) {
                    LOG.warn("More than one schema exists with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                }
                schemaVersionInfo = versionedSchemas.iterator().next().toSchemaVersionInfo();
            } else {
                throw new SchemaNotFoundException("No Schema version exists with schemaMetadataId " + schemaMetadataId + " and version " + version);
            }
        }

        return schemaVersionInfo;
    }

    public static class Options {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        public static final String SCHEMA_CACHE_SIZE = "schemaCacheSize";
        public static final String SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = "schemaCacheExpiryInterval";
        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 10000;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60L;

        private final Map<String, ?> config;

        public Options(Map<String, ?> config) {
            this.config = config;
        }

        private Object getPropertyValue(String propertyKey, Object defaultValue) {
            Object value = config.get(propertyKey);
            return value != null ? value : defaultValue;
        }

        public int getMaxSchemaCacheSize() {
            return Integer.valueOf(getPropertyValue(SCHEMA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE).toString());
        }

        public long getSchemaExpiryInSecs() {
            return Long.valueOf(getPropertyValue(SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS)
                                        .toString());
        }
    }
}
