package com.netflix.genie.core.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * This class abstracts away all the implementations of FileTransfer interface. It iterates through a list of
 * available implementations and tries to perform the file transfer operations.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Service
public class GenieFileTransferService {

    private final List<FileTransfer> fileTransferList;

    /**
     * Constructor.
     *
     * @param fileTransferImpls List of implementations of all fileTransfer interface
     *
     * @throws GenieException If there is any problem
     */
    @Autowired
    public GenieFileTransferService(
        final List<FileTransfer> fileTransferImpls
    ) throws GenieException {
        this.fileTransferList = fileTransferImpls; }

    /**
     * Get the file needed by Genie for job execution.
     *
     * @param srcRemotePath Path of the file in the remote location to be fetched
     * @param dstLocalPath Local path where the file needs to be placed
     * @throws GenieException If there is any problem
     */
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.")
        final String srcRemotePath,
        @NotBlank (message = "Destination local path cannot be empty")
        final String dstLocalPath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

        for (FileTransfer ft: fileTransferList) {
            if (ft.isValid(srcRemotePath)) {
                ft.getFile(srcRemotePath, dstLocalPath);
                return;
            }
        }

        throw new GenieNotFoundException("Could not find the appropriate FileTransfer implementation to get file"
            + srcRemotePath);
    }

    /**
     * Put the file provided by Genie.
     *
     * @param srcLocalPath The local path of the file which has to be transfered to remote location
     * @param dstRemotePath The remote destination path where the file has to be put
     * @throws GenieException If there is any problem
     */
    public void putFile(
        @NotBlank (message = "Source local path cannot be empty.")
        final String srcLocalPath,
        @NotBlank (message = "Destination remote path cannot be empty")
        final String dstRemotePath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);

        for (FileTransfer ft: fileTransferList) {
            if (ft.isValid(dstRemotePath)) {
                ft.putFile(srcLocalPath, dstRemotePath);
                return;
            }
        }

        throw new GenieNotFoundException("Could not find the appropriate FileTransfer implementation to get file"
            + dstRemotePath);
    }
}
