package io.jenkins.plugins.minio.upload;

import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.EnvVars;
import hudson.FilePath;
import hudson.Launcher;
import hudson.Util;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import io.jenkins.plugins.minio.ClientUtil;
import io.minio.*;
import io.minio.errors.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author Ronald Kamphuis
 */
public class MinioStepExecution {

    private final Run<?, ?> run;
    private final FilePath workspace;
    private final EnvVars env;
    private final Launcher launcher;
    private final TaskListener taskListener;
    private final MinioBuildStep step;

    public MinioStepExecution(@NonNull Run<?, ?> run, @NonNull FilePath workspace, @NonNull EnvVars env, @NonNull Launcher launcher,
                              @NonNull TaskListener taskListener, @NonNull MinioBuildStep step) {
        this.run = run;
        this.workspace = workspace;
        this.env = env;
        this.launcher = launcher;
        this.taskListener = taskListener;
        this.step = step;
    }

    public boolean start() throws Exception {

        MinioClient client = ClientUtil.getClient(step.getHost(), step.getCredentialsId(), run);

        String targetBucket = step.getBucket();
        ensureBucketExists(client, targetBucket);

        String includes = Util.replaceMacro(this.step.getIncludes(), env);
        String excludes = Util.replaceMacro(this.step.getExcludes(), env);
        String targetFolderExpanded = Util.replaceMacro(this.step.getTargetFolder(), env);

        if (!StringUtils.isEmpty(targetFolderExpanded) && !targetFolderExpanded.endsWith("/")) {
            targetFolderExpanded = targetFolderExpanded + "/";
        }

        final String targetFolder = targetFolderExpanded;
        FilePath startPath;
        if (StringUtils.isNotBlank(step.getStartFolder())) {
            startPath = this.workspace.child(step.getStartFolder());
        } else {
            startPath = this.workspace;
        }
        Arrays.asList(startPath.list(includes, excludes)).forEach(filePath -> {
            String filename;
            if (step.isPreserveFolderStructure()) {
                filename = getRelativePath(startPath, filePath);
            } else {
                filename = filePath.getName();
            }
            taskListener.getLogger().println(String.format("Storing %s in bucket %s", filename, targetBucket));
            try {
                PutObjectArgs put = PutObjectArgs.builder()
                        .bucket(this.step.getBucket())
                        .object(targetFolder + filename)
                        .stream(filePath.read(), filePath.toVirtualFile().length(), -1)
                        .contentType("application/octet-stream")
                        .build();
                client.putObject(put);

            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) { // Gotta catch 'em all
                run.setResult(Result.UNSTABLE);
            }
        });

        return true;
    }

    private String getRelativePath(FilePath startPath, FilePath filePath) {
        if (filePath == null) {
            return "";
        }
        Deque<String> trace = new ArrayDeque<>();
        while (!startPath.equals(filePath)) {
            trace.offer(filePath.getName());
            filePath = filePath.getParent();
        }
        StringBuilder result = new StringBuilder();
        String part;
        String slash = "";
        while (null != (part = trace.pollLast())) {
            result.append(slash);
            result.append(part);
            slash = "/";
        }
        return result.toString();
    }

    private void ensureBucketExists(MinioClient client, String targetBucket)
            throws ErrorResponseException, InsufficientDataException, InternalException, InvalidBucketNameException,
            InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException, ServerException,
            XmlParserException, RegionConflictException {
        if (!client.bucketExists(BucketExistsArgs.builder().bucket(targetBucket).build())) {
            client.makeBucket(MakeBucketArgs.builder().bucket(targetBucket).build());
        }
    }
}
