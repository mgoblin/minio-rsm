/*
 * Copyright 2023 Michael Golovanov <mike.golovanov@gmail.com>
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

package ru.mg.kafka.tieredstorage.minio.config;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.Validate;
import org.apache.commons.validator.routines.UrlValidator;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import ru.mg.kafka.tieredstorage.minio.DeferredInitRsm;

import okhttp3.HttpUrl;

import static io.minio.http.HttpUtils.validateUrl;

/**
 * Minio config for Remote storage manager
 *
 * <p>Parse and validate Remote storage manager configs
 *
 * <p>Supported config keys are
 * <ul>
 *     <li>minio.url - not blank required. Minio S3 endpoint URL</li>
 *     <li>minio.access.key  - Minio S3 access key. Non blank string, required. </li>
 *     <li>minio.secret.key  - Minio S3 secret key. Non blank string, required. </li>
 *     <li>minio.bucket.name - Minio bucket name. Non blank string, optional.
 *        Default value is "kafka-tiered-storage-bucket"
 *     </li>
 *     <li>minio.auto.create.bucket - create bucket if not exists. boolean, optional, default true</li>
 * </ul>
 *
 * @see DeferredInitRsm
 */
// TODO add minio client all params support
public class ConnectionConfig extends AbstractConfig {
    /**
     * Minio S3 endpoint URL config key name
     */
    public static final String MINIO_S3_ENDPOINT_URL = "minio.url";

    /**
     * Minio access key config key name
     */
    public static final String MINIO_ACCESS_KEY = "minio.access.key";

    /**
     * Minio secret key config key name
     */
    public static final String MINIO_SECRET_KEY = "minio.secret.key";

    /**
     * Bucket config key name
     */
    public static final String MINIO_BUCKET_NAME = "minio.bucket.name";

    /**
     * Default bucket name
     */
    public static final String MINIO_BUCKET_NAME_DEFAULT_VALUE = "kafka-tiered-storage-bucket";

    /**
     * Auto create bucket config key name
     */
    public static final String MINIO_AUTO_CREATE_BUCKET = "minio.auto.create.bucket";

    private static final String MINIO_S3_ENDPOINT_URL_DOC = "Minio S3 endpoint URL";
    private static final String MINIO_ACCESS_KEY_DOC = "Minio S3 endpoint access key";
    private static final String MINIO_SECRET_KEY_DOC = "Minio S3 endpoint secret key";
    private static final String MINIO_BUCKET_NAME_DOC = "Minio S3 bucket name for kafka tiered storage";
    private static final String MINIO_AUTO_CREATE_BUCKET_DOC = "Create bucket if not exists on broker start";
    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
                MINIO_S3_ENDPOINT_URL,
                ConfigDef.Type.STRING,
                "",
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                MINIO_S3_ENDPOINT_URL_DOC
        );

        CONFIG.define(
                MINIO_ACCESS_KEY,
                ConfigDef.Type.STRING,
                "",
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                MINIO_ACCESS_KEY_DOC
        );

        CONFIG.define(
                MINIO_SECRET_KEY,
                ConfigDef.Type.PASSWORD,
                "",
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                MINIO_SECRET_KEY_DOC
        );

        CONFIG.define(
                MINIO_BUCKET_NAME,
                ConfigDef.Type.STRING,
                MINIO_BUCKET_NAME_DEFAULT_VALUE,
                new ConfigDef.NonEmptyStringWithoutControlChars(),
                ConfigDef.Importance.HIGH,
                MINIO_BUCKET_NAME_DOC
        );

        CONFIG.define(
                MINIO_AUTO_CREATE_BUCKET,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                MINIO_AUTO_CREATE_BUCKET_DOC
        );
    }

    /**
     * Create and validate the configs
     *
     * @param props key-value properties map
     */
    public ConnectionConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        validate();
    }

    private void  validate() {
        validateMinioS3EndpointUrl();
        validateAccessKey();
        validateSecretKey();
        validateBucketName();
    }

    private void validateMinioS3EndpointUrl() {
        try {
            final String urlString = getString(MINIO_S3_ENDPOINT_URL);
            Validate.notBlank(urlString, String .format("%s should not be blank", MINIO_S3_ENDPOINT_URL));

            final String[] schemes = {"http", "https"};
            final UrlValidator urlValidator = new UrlValidator(schemes, null, 0);
            if (!urlValidator.isValid(urlString)) {
                throw new IllegalArgumentException(String.format("minio.url value %s is not valid URL", urlString));
            }

            final HttpUrl url = HttpUrl.parse(urlString);
            Objects.requireNonNull(url);

            validateUrl(url);
        } catch (final IllegalArgumentException | NullPointerException e) {
            throw new ConfigException(String.format(
                    "Remote storage manager config is not valid: %s", e.getMessage()));
        }
    }

    private void validateAccessKey() {
        try {
            final var accessKey = getString(MINIO_ACCESS_KEY);
            Validate.notBlank(accessKey, String.format("%s should not be blank", MINIO_ACCESS_KEY));
        } catch (final IllegalArgumentException | NullPointerException e) {
            throw new ConfigException(String.format(
                    "Remote storage manager config is not valid: %s", e.getMessage()));
        }
    }

    private void validateSecretKey() {
        try {
            final var secretKey = getPassword(MINIO_SECRET_KEY).value();
            Validate.notBlank(secretKey, String.format("%s should not be blank", MINIO_SECRET_KEY));
        } catch (final IllegalArgumentException | NullPointerException e) {
            throw new ConfigException(String.format(
                    "Remote storage manager config is not valid: %s", e.getMessage()));
        }
    }

    private void validateBucketName() {
        try {
            final var bucketName = getString(MINIO_BUCKET_NAME);
            Validate.notBlank(bucketName, String.format("%s should not be blank", MINIO_BUCKET_NAME));
        } catch (final IllegalArgumentException | NullPointerException e) {
            throw new ConfigException(String.format(
                    "Remote storage manager config is not valid: %s", e.getMessage()));
        }
    }

    public String getMinioS3EndpointUrl() {
        return getString(MINIO_S3_ENDPOINT_URL);
    }

    public String getMinioAccessKey() {
        return getString(MINIO_ACCESS_KEY);
    }

    public Password getMinioSecretKey() {
        return getPassword(MINIO_SECRET_KEY);
    }

    public String getMinioBucketName() {
        return getString(MINIO_BUCKET_NAME);
    }

    public boolean isAutoCreateBucket() {
        return getBoolean(MINIO_AUTO_CREATE_BUCKET);
    }

    @Override
    public String toString() {
        return "values: " + System.lineSeparator()
                + "\tminio.endpoint = " + this.getMinioS3EndpointUrl() + System.lineSeparator()
                + "\tminio.bucketName = " + this.getMinioBucketName() + System.lineSeparator()
                + "\tminio.auto.create.bucket = " + this.isAutoCreateBucket() + System.lineSeparator()
                + "\tminio.access.key = " + this.getMinioAccessKey() + System.lineSeparator()
                + "\tminio.secret.key = [hidden]" + System.lineSeparator();
    }
}
