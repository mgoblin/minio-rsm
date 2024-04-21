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

package ru.mg.kafka.tieredstorage.minio.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.minio.GetObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.backend.IFetcher;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO Add unit tests

/**
 * Fetcher implementation for Minio S3
 *
 * @see IFetcher
 * @see ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend
 */
public class Fetcher extends BackendPart implements IFetcher {
    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    public Fetcher(final ConnectionConfig config) {
        super(config);
    }

    /**
     * Fetches all bytes from S3 object
     *
     * @param segmentObjectName object name
     * @return byte array with data
     * @throws RemoteStorageException on error
     */
    public byte[] fetchAllBytes(final String segmentObjectName) throws RemoteStorageException {
        try {
            final var response = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(segmentObjectName)
                            .build());

            final byte[] body = response.readAllBytes();

            log.debug("Fetch bytes from path {} success. "
                            + "Fetched {} bytes.",
                    segmentObjectName,
                    body.length);

            return body;

        } catch (final IOException e) {
            log.error("Fetch log segment data from object {} failed. IO exception occurred.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ServerException e) {
            log.error("Fetch log segment data from object {} failed. Http server error.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error("Fetch log segment data from object {} failed. Insufficient data.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. "
                            + "Error response with code {} and message {}.",
                    config.getMinioBucketName(),
                    segmentObjectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException
                       | XmlParserException | InternalException | InvalidResponseException e) {
            log.error("Fetch log segment data from object {} failed. Internal error occurred.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        }
    }

    /**
     * Fetchers log segment data from S3  from start to end position
     *
     * @param segmentObjectName - object name in S3 remote storage
     * @param startPosition start position
     * @param endPosition end position
     * @return log segment input stream
     * @throws RemoteStorageException on error
     */
    public InputStream fetchLogSegmentData(
            final String segmentObjectName,
            final int startPosition,
            final int endPosition) throws RemoteStorageException {

        final byte[] body = fetchAllBytes(segmentObjectName);

        final byte[] subArray = Arrays.copyOfRange(body, startPosition, endPosition);
        log.debug("Fetch log segment data from start and end positions {} - {} with path {} success. "
                        + "Fetched {} bytes, trimmed to {}.",
                startPosition,
                endPosition,
                segmentObjectName,
                body.length,
                subArray.length);

        return new ByteArrayInputStream(subArray);
    }

    /**
     * Fetches log segment data from Minio S3 from start position
     *
     * @param segmentObjectName object name
     * @param startPosition start position
     * @return log segment input stream
     * @throws RemoteStorageException on error
     */
    public InputStream fetchLogSegmentData(
            final String segmentObjectName,
            final int startPosition) throws RemoteStorageException {

        final byte[] body = fetchAllBytes(segmentObjectName);

        final byte[] subArray = Arrays.copyOfRange(body, startPosition, body.length);
        log.debug("Fetch log segment data from start position {} with path {} success. "
                        + "Fetched {} bytes, trimmed to {}.",
                startPosition,
                segmentObjectName,
                body.length,
                subArray.length);

        return new ByteArrayInputStream(subArray);
    }

    /**
     * Fetches index by type from S3 Minio
     *
     * @param indexObjectName object name
     * @param indexType index type
     * @return index data input stream
     * @throws RemoteStorageException on error
     */
    public InputStream readIndex(final String indexObjectName, final RemoteStorageManager.IndexType indexType)
            throws RemoteStorageException {

        try {
            final var response = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(indexObjectName)
                            .build());

            final byte[] body = response.readAllBytes();
            return new ByteArrayInputStream(body);
        } catch (final IOException e) {
            log.error("Fetch index {} from {} failed. IO exception occurred.", indexType, indexObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ServerException e) {
            log.error("Minio S3 bucket {} server operation on get {} failed with http code {}.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    e.statusCode(),
                    e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. "
                            + "Not enough data available in InputStream.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. "
                            + "Error response with code {} and message {}.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException | InternalException e) {
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. Internal minio error.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        }
    }

}
