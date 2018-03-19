/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.Lists;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.services.DownloadService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

/**
 * Command to download dependencies as a job would.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class DownloadCommand implements AgentCommand {

    private final DownloadCommandArguments downloadCommandArguments;
    private final DownloadService downloadService;

    DownloadCommand(
        final DownloadCommandArguments downloadCommandArguments,
        final DownloadService downloadService
    ) {
        this.downloadCommandArguments = downloadCommandArguments;
        this.downloadService = downloadService;
    }

    @Override
    public void run() {

        final List<URI> uris = downloadCommandArguments.getFileUris();
        final File destinationDirectory = downloadCommandArguments.getDestinationDirectory();

        if (!destinationDirectory.exists() || !destinationDirectory.isDirectory()) {
            throw new ParameterException(
                "Not a valid destination directory: "
                + destinationDirectory.getAbsolutePath()
            );
        }

        log.info(
            "Downloading {} files into: {}",
            uris.size(),
            downloadCommandArguments.getDestinationDirectory()
        );


        final DownloadService.Manifest.Builder manifestBuilder = downloadService.newManifestBuilder();

        for (final URI uri : uris) {
            log.info(" * {}", uri);
            manifestBuilder.addFileWithTargetDirectory(uri, destinationDirectory);
        }

        final DownloadService.Manifest manifest = manifestBuilder.build();

        try {
            downloadService.download(manifest);
        } catch (final DownloadException e) {
            throw new RuntimeException("Download failed", e);
        }
    }

    @Component
    @Parameters(commandNames = CommandNames.DOWNLOAD, commandDescription = "Download a set of files")
    static class DownloadCommandArguments implements AgentCommandArguments {

        @ParametersDelegate
        @Getter
        private final ArgumentDelegates.CacheArguments cacheArguments;

        @Parameter(
            names = {"--sources"},
            description = "URLs of files to download",
            validateWith = ArgumentValidators.StringValidator.class,
            converter = ArgumentConverters.URIConverter.class,
            variableArity = true
        )
        @Getter
        private List<URI> fileUris;

        @Parameter(
            names = {"--destinationDirectory"},
            validateWith = ArgumentValidators.StringValidator.class,
            converter = ArgumentConverters.FileConverter.class
        )
        @Getter
        private File destinationDirectory;


        DownloadCommandArguments(
            final ArgumentDelegates.CacheArguments cacheArguments
        ) {
            this.cacheArguments = cacheArguments;
            this.fileUris = Lists.newArrayList();
            this.destinationDirectory = Paths.get("").toAbsolutePath().toFile();
        }

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return DownloadCommand.class;
        }
    }
}
