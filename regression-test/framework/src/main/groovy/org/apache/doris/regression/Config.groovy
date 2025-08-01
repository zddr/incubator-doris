// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.regression

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import com.google.common.collect.Maps
import org.apache.commons.cli.CommandLine
import org.apache.doris.regression.util.FileUtils
import org.apache.doris.regression.util.JdbcUtils

import java.sql.Connection
import java.sql.DriverManager
import java.util.function.Predicate

import static org.apache.doris.regression.ConfigOptions.*

import org.apache.doris.thrift.TNetworkAddress;

enum RunMode {
    UNKNOWN,
    NOT_CLOUD,
    CLOUD
}

@Slf4j
@CompileStatic
class Config {
    public String jdbcUrl
    public String jdbcUser
    public String jdbcPassword

    public String defaultDb

    public String ccrDownstreamUrl
    public String ccrDownstreamUser
    public String ccrDownstreamPassword

    public String feSourceThriftAddress
    public String feTargetThriftAddress
    public String feSyncerUser
    public String feSyncerPassword
    public String syncerAddress

    public String feHttpAddress
    public String feHttpUser
    public String feHttpPassword

    public String feCloudHttpAddress
    public String feCloudHttpUser
    public String feCloudHttpPassword

    public String instanceId
    public String cloudUniqueId
    public String metaServiceHttpAddress
    public String recycleServiceHttpAddress

    public RunMode runMode = RunMode.UNKNOWN

    public String suitePath
    public String dataPath
    public String realDataPath
    public String cacheDataPath
    public boolean enableCacheData
    public String pluginPath
    public String sslCertificatePath
    public String dorisComposePath
    public String image
    public String dockerCoverageOutputDir
    public Boolean dockerEndDeleteFiles
    public Boolean dockerEndNoKill
    public Boolean excludeDockerTest

    public String testGroups
    public String excludeGroups
    public String testSuites
    public String excludeSuites
    public String testDirectories
    public String excludeDirectories
    public boolean generateOutputFile
    public boolean forceGenerateOutputFile
    public boolean randomOrder
    public boolean stopWhenFail
    public boolean dryRun

    public Properties otherConfigs = new Properties()

    public Set<String> suiteWildcard = new HashSet<>()
    public Set<String> groups = new HashSet<>()
    public Set<String> directories = new HashSet<>()

    public Set<String> excludeSuiteWildcard = new HashSet<>()
    public Set<String> excludeGroupSet = new HashSet<>()
    public Set<String> excludeDirectorySet = new HashSet<>()

    public TNetworkAddress feSourceThriftNetworkAddress
    public TNetworkAddress feTargetThriftNetworkAddress
    public TNetworkAddress syncerNetworkAddress
    public InetSocketAddress feHttpInetSocketAddress
    public InetSocketAddress feCloudHttpInetSocketAddress
    public InetSocketAddress metaServiceHttpInetSocketAddress
    public InetSocketAddress recycleServiceHttpInetSocketAddress
    public Integer parallel
    public Integer suiteParallel
    public Integer dockerSuiteParallel
    public Integer actionParallel
    public Integer times
    public boolean withOutLoadData
    public boolean runNonConcurrent
    public String caseNamePrefix
    public String validateBackupPrefix
    public boolean isSmokeTest
    public String multiClusterBes
    public String metaServiceToken
    public String multiClusterInstance
    public String upgradeNewBeIp
    public String upgradeNewBeHbPort
    public String upgradeNewBeHttpPort
    public String upgradeNewBeUniqueId

    public String stageIamEndpoint
    public String stageIamRegion
    public String stageIamBucket
    public String stageIamPolicy
    public String stageIamRole
    public String stageIamArn
    public String stageIamAk
    public String stageIamSk
    public String stageIamUserId

    public String clusterDir
    public String kafkaBrokerList
    public String cloudVersion

    public String s3Source

    // for aws role arn regression test
    public String awsRoleArn
    public String awsExternalId
    public String awsEndpoint
    public String awsRegion
    public String awsBucket
    public String awsPrefix
    public String awsAccessKey
    public String awsSecretKey

    public String regressionAwsRoleArn
    public String regressionAwsExternalId
    public String regressionAwsEndpoint
    public String regressionAwsRegion
    public String regressionAwsBucket
    public String regressionAwsPrefix

    Config() {}

    Config(
            String s3Source,
            String caseNamePrefix,
            String validateBackupPrefix,
            String defaultDb, 
            String jdbcUrl, 
            String jdbcUser,
            String jdbcPassword,
            String feSourceThriftAddress,
            String feTargetThriftAddress,
            String feSyncerUser,
            String feSyncerPassword,
            String syncerPassword,
            String feHttpAddress,
            String feHttpUser,
            String feHttpPassword, 
            String feCloudHttpAddress,
            String feCloudHttpUser,
            String feCloudHttpPassword,
            String instanceId,
            String cloudUniqueId,
            String metaServiceHttpAddress,
            String recycleServiceHttpAddress,
            String suitePath,
            String dataPath,
            String realDataPath,
            String cacheDataPath,
            Boolean enableCacheData,
            String testGroups,
            String excludeGroups,
            String testSuites, 
            String excludeSuites,
            String testDirectories,
            String excludeDirectories, 
            String pluginPath,
            String sslCertificatePath,
            String multiClusterBes,
            String metaServiceToken,
            String multiClusterInstance,
            String upgradeNewBeIp, 
            String upgradeNewBeHbPort,
            String upgradeNewBeHttpPort,
            String upgradeNewBeUniqueId,
            String stageIamEndpoint,
            String stageIamRegion,
            String stageIamBucket,
            String stageIamPolicy,
            String stageIamRole,
            String stageIamArn,
            String stageIamAk,
            String stageIamSk,
            String stageIamUserId,
            String clusterDir, 
            String kafkaBrokerList, 
            String cloudVersion) {
        this.s3Source = s3Source
        this.caseNamePrefix = caseNamePrefix
        this.validateBackupPrefix = validateBackupPrefix
        this.defaultDb = defaultDb
        this.jdbcUrl = jdbcUrl
        this.jdbcUser = jdbcUser
        this.jdbcPassword = jdbcPassword
        this.feSourceThriftAddress = feSourceThriftAddress
        this.feTargetThriftAddress = feTargetThriftAddress
        this.feSyncerUser = feSyncerUser
        this.feSyncerPassword = feSyncerPassword
        this.syncerAddress = syncerAddress
        this.feHttpAddress = feHttpAddress
        this.feHttpUser = feHttpUser
        this.feHttpPassword = feHttpPassword
        this.feCloudHttpAddress = feCloudHttpAddress
        this.feCloudHttpUser = feCloudHttpUser
        this.feCloudHttpPassword = feCloudHttpPassword
        this.instanceId = instanceId
        this.cloudUniqueId = cloudUniqueId
        this.metaServiceHttpAddress = metaServiceHttpAddress
        this.recycleServiceHttpAddress = recycleServiceHttpAddress
        this.suitePath = suitePath
        this.dataPath = dataPath
        this.realDataPath = realDataPath
        this.cacheDataPath = cacheDataPath
        this.enableCacheData = enableCacheData
        this.testGroups = testGroups
        this.excludeGroups = excludeGroups
        this.testSuites = testSuites
        this.excludeSuites = excludeSuites
        this.testDirectories = testDirectories
        this.excludeDirectories = excludeDirectories
        this.pluginPath = pluginPath
        this.sslCertificatePath = sslCertificatePath
        this.multiClusterBes = multiClusterBes
        this.metaServiceToken = metaServiceToken
        this.multiClusterInstance = multiClusterInstance
        this.upgradeNewBeIp = upgradeNewBeIp
        this.upgradeNewBeHbPort = upgradeNewBeHbPort
        this.upgradeNewBeHttpPort = upgradeNewBeHttpPort
        this.upgradeNewBeUniqueId = upgradeNewBeUniqueId
        this.stageIamEndpoint = stageIamEndpoint
        this.stageIamRegion = stageIamRegion
        this.stageIamBucket = stageIamBucket
        this.stageIamPolicy = stageIamPolicy
        this.stageIamRole = stageIamRole
        this.stageIamArn = stageIamArn
        this.stageIamAk = stageIamAk
        this.stageIamSk = stageIamSk
        this.stageIamUserId = stageIamUserId
        this.clusterDir = clusterDir
        this.kafkaBrokerList = kafkaBrokerList
        this.cloudVersion = cloudVersion
    }

    static String removeDirectoryPrefix(String str) {
        def prefixes = ["./regression-test/suites/", "regression-test/suites/"]

        def prefix = prefixes.find { str.startsWith(it) }
        if (prefix) {
            return str.substring(prefix.length())
        }

        return str
    }

    static Config fromCommandLine(CommandLine cmd) {
        String confFilePath = cmd.getOptionValue(confFileOpt, "")
        File confFile = new File(confFilePath)
        Config config = new Config()
        if (confFile.exists() && confFile.isFile()) {
            log.info("Load config file ${confFilePath}".toString())
            def configSlurper = new ConfigSlurper()
            def systemProperties = Maps.newLinkedHashMap(System.getProperties())
            configSlurper.setBinding(systemProperties)
            ConfigObject configObj = configSlurper.parse(new File(confFilePath).toURI().toURL())
            String customConfFilePath = confFile.getParentFile().getPath() + "/regression-conf-custom.groovy"
            File custFile = new File(customConfFilePath)
            if (custFile.exists() && custFile.isFile()) {
                ConfigObject custConfigObj = configSlurper.parse(new File(customConfFilePath).toURI().toURL())
                configObj.merge(custConfigObj)
            }
            config = Config.fromConfigObject(configObj)
        }
        fillDefaultConfig(config)

        config.suitePath = FileUtils.getCanonicalPath(cmd.getOptionValue(pathOpt, config.suitePath))
        config.dataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(dataOpt, config.dataPath))
        config.realDataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(realDataOpt, config.realDataPath))
        config.cacheDataPath = cmd.getOptionValue(cacheDataOpt, config.cacheDataPath)
        config.enableCacheData = Boolean.parseBoolean(cmd.getOptionValue(enableCacheDataOpt, config.enableCacheData.toString()))
        config.pluginPath = FileUtils.getCanonicalPath(cmd.getOptionValue(pluginOpt, config.pluginPath))
        config.sslCertificatePath = FileUtils.getCanonicalPath(cmd.getOptionValue(sslCertificateOpt, config.sslCertificatePath))
        config.dorisComposePath = FileUtils.getCanonicalPath(config.dorisComposePath)
        config.image = cmd.getOptionValue(imageOpt, config.image)
        config.dockerEndNoKill = cmd.hasOption(noKillDockerOpt)
        if (cmd.hasOption(runModeOpt)) {
            String runMode = cmd.getOptionValue(runModeOpt, "unknown")
            if (runMode.equalsIgnoreCase("unknown")) {
                config.runMode = RunMode.UNKNOWN;
            } else if (runMode.equalsIgnoreCase("cloud")) {
                config.runMode = RunMode.CLOUD;
            } else if (runMode.equalsIgnoreCase("not_cloud")) {
                config.runMode = RunMode.NOT_CLOUD;
            } else {
                throw new IllegalStateException("Bad runMode: ${runMode}, should be one of unknown/cloud/not_cloud, "
                        + "if is unknown, fetch it from fe")
            }
        }
        log.info("runMode: ${config.runMode}")
        config.suiteWildcard = cmd.getOptionValue(suiteOpt, config.testSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.groups = cmd.getOptionValue(groupsOpt, config.testGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.directories = cmd.getOptionValue(directoriesOpt, config.testDirectories)
                .split(",")
                .collect({d -> 
                    d.trim()
                    removeDirectoryPrefix(d)})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()
        config.excludeSuiteWildcard = cmd.getOptionValue(excludeSuiteOpt, config.excludeSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.excludeGroupSet = cmd.getOptionValue(excludeGroupsOpt, config.excludeGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.excludeDirectorySet = cmd.getOptionValue(excludeDirectoriesOpt, config.excludeDirectories)
                .split(",")
                .collect({d -> d.trim()})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()

        if (!config.suiteWildcard && !config.groups && !config.directories && !config.excludeSuiteWildcard
            && !config.excludeGroupSet && !config.excludeDirectorySet) {
            log.info("no suites/directories/groups specified, set groups to p0".toString())
            config.groups = ["p0"].toSet()
        }

        config.feSourceThriftAddress = cmd.getOptionValue(feSourceThriftAddressOpt, config.feSourceThriftAddress)
        try {
            String host = config.feSourceThriftAddress.split(":")[0]
            int port = Integer.valueOf(config.feSourceThriftAddress.split(":")[1])
            config.feSourceThriftNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe thrift address: ${config.feSourceThriftAddress}", t)
        }

        config.feTargetThriftAddress = cmd.getOptionValue(feTargetThriftAddressOpt, config.feTargetThriftAddress)
        try {
            String host = config.feTargetThriftAddress.split(":")[0]
            int port = Integer.valueOf(config.feTargetThriftAddress.split(":")[1])
            config.feTargetThriftNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe thrift address: ${config.feTargetThriftAddress}", t)
        }

        config.syncerAddress = cmd.getOptionValue(syncerAddressOpt, config.syncerAddress)
        try {
            String host = config.syncerAddress.split(":")[0]
            int port = Integer.valueOf(config.syncerAddress.split(":")[1])
            config.syncerNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse syncer address: ${config.syncerAddress}", t)
        }

        config.feHttpAddress = cmd.getOptionValue(feHttpAddressOpt, config.feHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feHttpAddress.split(":")[1])
            config.feHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse stream load address: ${config.feHttpAddress}", t)
        }

        config.feCloudHttpAddress = cmd.getOptionValue(feCloudHttpAddressOpt, config.feCloudHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feCloudHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feCloudHttpAddress.split(":")[1])
            config.feCloudHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe cloud http address: ${config.feCloudHttpAddress}", t)
        }
        log.info("feCloudHttpAddress : $config.feCloudHttpAddress, socketAddr : $config.feCloudHttpInetSocketAddress")

        config.instanceId = cmd.getOptionValue(instanceIdOpt, config.instanceId)
        log.info("instanceId : ${config.instanceId}")

        config.cloudUniqueId = cmd.getOptionValue(cloudUniqueIdOpt, config.cloudUniqueId)
        log.info("cloudUniqueId : ${config.cloudUniqueId}")

        config.metaServiceHttpAddress = cmd.getOptionValue(metaServiceHttpAddressOpt, config.metaServiceHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.metaServiceHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.metaServiceHttpAddress.split(":")[1])
            config.metaServiceHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse meta service address: ${config.metaServiceHttpAddress}", t)
        }
        log.info("msAddr : $config.metaServiceHttpAddress, socketAddr : $config.metaServiceHttpInetSocketAddress")

                config.multiClusterBes = cmd.getOptionValue(multiClusterBesOpt, config.multiClusterBes)
        log.info("multiClusterBes is ${config.multiClusterBes}".toString())

        config.metaServiceToken = cmd.getOptionValue(metaServiceTokenOpt, config.metaServiceToken)
        log.info("metaServiceToken is ${config.metaServiceToken}".toString())

        config.multiClusterInstance = cmd.getOptionValue(multiClusterInstanceOpt, config.multiClusterInstance)
        log.info("multiClusterInstance is ${config.multiClusterInstance}".toString())

        config.upgradeNewBeIp = cmd.getOptionValue(upgradeNewBeIpOpt, config.upgradeNewBeIp)
        log.info("upgradeNewBeIp is ${config.upgradeNewBeIp}".toString())

        config.upgradeNewBeHbPort = cmd.getOptionValue(upgradeNewBeHbPortOpt, config.upgradeNewBeHbPort)
        log.info("upgradeNewBeHbPort is ${config.upgradeNewBeHbPort}".toString())

        config.upgradeNewBeHttpPort = cmd.getOptionValue(upgradeNewBeHttpPortOpt, config.upgradeNewBeHttpPort)
        log.info("upgradeNewBeHttpPort is ${config.upgradeNewBeHttpPort}".toString())

        config.upgradeNewBeUniqueId = cmd.getOptionValue(upgradeNewBeUniqueIdOpt, config.upgradeNewBeUniqueId)
        log.info("upgradeNewBeUniqueId is ${config.upgradeNewBeUniqueId}".toString())

        config.stageIamEndpoint = cmd.getOptionValue(stageIamEndpointOpt, config.stageIamEndpoint)
        log.info("stageIamEndpoint is ${config.stageIamEndpoint}".toString())
        config.stageIamRegion = cmd.getOptionValue(stageIamRegionOpt, config.stageIamRegion)
        log.info("stageIamRegion is ${config.stageIamRegion}".toString())
        config.stageIamBucket = cmd.getOptionValue(stageIamBucketOpt, config.stageIamBucket)
        log.info("stageIamBucket is ${config.stageIamBucket}".toString())
        config.stageIamPolicy = cmd.getOptionValue(stageIamPolicyOpt, config.stageIamPolicy)
        log.info("stageIamPolicy is ${config.stageIamPolicy}".toString())
        config.stageIamRole = cmd.getOptionValue(stageIamRoleOpt, config.stageIamRole)
        log.info("stageIamRole is ${config.stageIamRole}".toString())
        config.stageIamArn = cmd.getOptionValue(stageIamArnOpt, config.stageIamArn)
        log.info("stageIamArn is ${config.stageIamArn}".toString())
        config.stageIamAk = cmd.getOptionValue(stageIamAkOpt, config.stageIamAk)
        log.info("stageIamAk is ${config.stageIamAk}".toString())
        config.stageIamSk = cmd.getOptionValue(stageIamSkOpt, config.stageIamSk)
        log.info("stageIamSk is ${config.stageIamSk}".toString())
        config.stageIamUserId = cmd.getOptionValue(stageIamUserIdOpt, config.stageIamUserId)
        log.info("stageIamUserId is ${config.stageIamUserId}".toString())
        config.cloudVersion = cmd.getOptionValue(cloudVersionOpt, config.cloudVersion)
        log.info("cloudVersion is ${config.cloudVersion}".toString())

        config.kafkaBrokerList = cmd.getOptionValue(kafkaBrokerListOpt, config.kafkaBrokerList)

        config.recycleServiceHttpAddress = cmd.getOptionValue(recycleServiceHttpAddressOpt, config.recycleServiceHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.recycleServiceHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.recycleServiceHttpAddress.split(":")[1])
            config.recycleServiceHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse recycle service address: ${config.recycleServiceHttpAddress}", t)
        }
        log.info("recycleAddr : $config.recycleServiceHttpAddress, socketAddr : $config.recycleServiceHttpInetSocketAddress")

        config.defaultDb = cmd.getOptionValue(defaultDbOpt, config.defaultDb)
        config.jdbcUrl = cmd.getOptionValue(jdbcOpt, config.jdbcUrl)
        config.jdbcUser = cmd.getOptionValue(userOpt, config.jdbcUser)
        config.jdbcPassword = cmd.getOptionValue(passwordOpt, config.jdbcPassword)
        config.feSyncerUser = cmd.getOptionValue(feSyncerUserOpt, config.feSyncerUser)
        config.feSyncerPassword = cmd.getOptionValue(feSyncerPasswordOpt, config.feSyncerPassword)
        config.feHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feHttpUser)
        config.feHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feHttpPassword)
        config.feCloudHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feCloudHttpUser)
        config.feCloudHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feCloudHttpPassword)
        config.generateOutputFile = cmd.hasOption(genOutOpt)
        config.forceGenerateOutputFile = cmd.hasOption(forceGenOutOpt)
        config.parallel = Integer.parseInt(cmd.getOptionValue(parallelOpt, "10"))
        config.suiteParallel = Integer.parseInt(cmd.getOptionValue(suiteParallelOpt, "10"))
        config.dockerSuiteParallel = Integer.parseInt(cmd.getOptionValue(dockerSuiteParallelOpt, "1"))
        config.actionParallel = Integer.parseInt(cmd.getOptionValue(actionParallelOpt, "10"))
        config.times = Integer.parseInt(cmd.getOptionValue(timesOpt, "1"))
        config.randomOrder = cmd.hasOption(randomOrderOpt)
        config.stopWhenFail = cmd.hasOption(stopWhenFailOpt)
        config.withOutLoadData = cmd.hasOption(withOutLoadDataOpt)
        config.runNonConcurrent = Boolean.parseBoolean(cmd.getOptionValue(runNonConcurrentOpt, "True"))
        config.caseNamePrefix = cmd.getOptionValue(caseNamePrefixOpt, config.caseNamePrefix)
        config.validateBackupPrefix = cmd.getOptionValue(validateBackupPrefixOpt, config.validateBackupPrefix)
        config.dryRun = cmd.hasOption(dryRunOpt)
        config.isSmokeTest = cmd.hasOption(isSmokeTestOpt)

        log.info("randomOrder is ${config.randomOrder}".toString())
        log.info("stopWhenFail is ${config.stopWhenFail}".toString())
        log.info("withOutLoadData is ${config.withOutLoadData}".toString())
        log.info("runNonConcurrent is ${config.runNonConcurrent}".toString())
        log.info("caseNamePrefix is ${config.caseNamePrefix}".toString())
        log.info("validateBackupPrefix is ${config.validateBackupPrefix}".toString())
        log.info("dryRun is ${config.dryRun}".toString())
        def s3SourceList = ["aliyun", "aliyun-internal", "tencent", "huawei", "azure", "gcp"]
        if (s3SourceList.contains(config.s3Source)) {
            log.info("s3Source is ${config.s3Source}".toString())
            log.info("s3Provider is ${config.otherConfigs.get("s3Provider")}".toString())
            log.info("s3BucketName is ${config.otherConfigs.get("s3BucketName")}".toString())
            log.info("s3Region is ${config.otherConfigs.get("s3Region")}".toString())
            log.info("s3Endpoint is ${config.otherConfigs.get("s3Endpoint")}".toString())
        } else {
            throw new Exception("The s3Source '${config.s3Source}' is invalid, optional values ${s3SourceList}")
        }

        Properties props = cmd.getOptionProperties("conf")
        config.otherConfigs.putAll(props)

        // mainly auth_xxx cases use defaultDb, these suites better not use defaultDb
        config.createDefaultDb()

        try {
            config.fetchCloudMode()
        } catch (Exception e) {
            // docker suite no need external cluster.
            // so can ignore error here.
        }
        config.excludeUnsupportedCase()
        return config
    }

    static Config fromConfigObject(ConfigObject obj) {
        def config = new Config(
            configToString(obj.s3Source),
            configToString(obj.caseNamePrefix),
            configToString(obj.validateBackupPrefix),
            configToString(obj.defaultDb),
            configToString(obj.jdbcUrl),
            configToString(obj.jdbcUser),
            configToString(obj.jdbcPassword),
            configToString(obj.feSourceThriftAddress),
            configToString(obj.feTargetThriftAddress),
            configToString(obj.feSyncerUser),
            configToString(obj.feSyncerPassword),
            configToString(obj.syncerAddress),
            configToString(obj.feHttpAddress),
            configToString(obj.feHttpUser),
            configToString(obj.feHttpPassword),
            configToString(obj.feCloudHttpAddress),
            configToString(obj.feCloudHttpUser),
            configToString(obj.feCloudHttpPassword),
            configToString(obj.instanceId),
            configToString(obj.cloudUniqueId),
            configToString(obj.metaServiceHttpAddress),
            configToString(obj.recycleServiceHttpAddress),
            configToString(obj.suitePath),
            configToString(obj.dataPath),
            configToString(obj.realDataPath),
            configToString(obj.cacheDataPath),
            configToBoolean(obj.enableCacheData),
            configToString(obj.testGroups),
            configToString(obj.excludeGroups),
            configToString(obj.testSuites),
            configToString(obj.excludeSuites),
            configToString(obj.testDirectories),
            configToString(obj.excludeDirectories),
            configToString(obj.pluginPath),
            configToString(obj.sslCertificatePath),
            configToString(obj.multiClusterBes),
            configToString(obj.metaServiceToken),
            configToString(obj.multiClusterInstance),
            configToString(obj.upgradeNewBeIp),
            configToString(obj.upgradeNewBeHbPort),
            configToString(obj.upgradeNewBeHttpPort),
            configToString(obj.upgradeNewBeUniqueId),
            configToString(obj.stageIamEndpoint),
            configToString(obj.stageIamRegion),
            configToString(obj.stageIamBucket),
            configToString(obj.stageIamPolicy),
            configToString(obj.stageIamRole),
            configToString(obj.stageIamArn),
            configToString(obj.stageIamAk),
            configToString(obj.stageIamSk),
            configToString(obj.stageIamUserId),
            configToString(obj.clusterDir),
            configToString(obj.kafkaBrokerList),
            configToString(obj.cloudVersion)
        )

        config.ccrDownstreamUrl = configToString(obj.ccrDownstreamUrl)
        config.ccrDownstreamUser = configToString(obj.ccrDownstreamUser)
        config.ccrDownstreamPassword = configToString(obj.ccrDownstreamPassword)
        config.image = configToString(obj.image)
        config.dockerCoverageOutputDir = configToString(obj.dockerCoverageOutputDir)
        config.dockerEndDeleteFiles = configToBoolean(obj.dockerEndDeleteFiles)
        config.dockerEndNoKill = configToBoolean(obj.dockerEndNoKill)
        config.excludeDockerTest = configToBoolean(obj.excludeDockerTest)

        config.awsRoleArn = configToString(obj.awsRoleArn)
        config.awsExternalId = configToString(obj.awsExternalId)
        config.awsPrefix = configToString(obj.awsPrefix)
        config.awsEndpoint = configToString(obj.awsEndpoint)
        config.awsRegion = configToString(obj.awsRegion)
        config.awsBucket = configToString(obj.awsBucket)
        config.awsAccessKey = configToString(obj.awsAccessKey)
        config.awsSecretKey = configToString(obj.awsSecretKey)
        config.awsPrefix = configToString(obj.awsPrefix)

        config.regressionAwsRoleArn = configToString(obj.regressionAwsRoleArn)
        config.regressionAwsExternalId = configToString(obj.regressionAwsExternalId)
        config.regressionAwsEndpoint = configToString(obj.regressionAwsEndpoint)
        config.regressionAwsRegion = configToString(obj.regressionAwsRegion)
        config.regressionAwsBucket = configToString(obj.regressionAwsBucket)
        config.regressionAwsPrefix = configToString(obj.regressionAwsPrefix)

        def declareFileNames = config.getClass()
                .getDeclaredFields()
                .collect({f -> f.name})
                .toSet()
        for (def kv : obj.toProperties().entrySet()) {
            String key = kv.getKey() as String
            if (!declareFileNames.contains(key)) {
                config.otherConfigs.put(key, kv.getValue())
            }
        }

        // check smoke config
        if (obj.isSmokeTest) {
            config.isSmokeTest = true
            String env = config.otherConfigs.getOrDefault("smokeEnv", "UNKNOWN")
            log.info("Start to check $env config")
            def c = config.otherConfigs
            c.put("feCloudHttpAddress", obj.feCloudHttpAddress)
            checkCloudSmokeEnv(c)
        }

        return config
    }

    static void checkCloudSmokeEnv(Properties properties) {
        // external stage obj info
        String s3Endpoint = properties.getOrDefault("s3Endpoint", "")
        String feCloudHttpAddress = properties.getOrDefault("feCloudHttpAddress", "")
        String s3Region = properties.getOrDefault("s3Region", "")
        String s3BucketName = properties.getOrDefault("s3BucketName", "")
        String s3AK = properties.getOrDefault("ak", "")
        String s3SK = properties.getOrDefault("sk", "")

        def items = [
                fecloudHttpAddrConf:feCloudHttpAddress,
                s3RegionConf:s3Region,
                s3EndpointConf:s3Endpoint,
                s3BucketConf:s3BucketName,
                s3AKConf:s3AK,
                s3SKConf:s3SK
        ]
        for (final def item in items) {
            if (item.value == null || item.value.isEmpty()) {
                throw new IllegalStateException("cloud smoke conf err, plz check " + item.key)
            }
        }
    }

    static void fillDefaultConfig(Config config) {
        if (config.s3Source == null) {
            config.s3Source = "aliyun"
            log.info("Set s3Source to 'aliyun' because not specify.".toString())
        }

        if (config.otherConfigs.get("s3Provider") == null) {
            def s3Provider = "OSS"
            if (config.s3Source == "aliyun" || config.s3Source == "aliyun-internal") {
                s3Provider = "OSS"
            } else if (config.s3Source == "tencent") {
                s3Provider = "COS"
            } else if (config.s3Source == "huawei") {
                s3Provider = "OBS"
            } else if (config.s3Source == "azure") {
                s3Provider = "Azure" // case insensitive test
            } else if (config.s3Source == "gcp") {
                s3Provider = "GCP"
            }
            config.otherConfigs.put("s3Provider", "${s3Provider}")
            log.info("Set s3Provider to '${s3Provider}' because not specify.".toString())
        }
        if (config.otherConfigs.get("s3BucketName") == null) {
            def s3BucketName = "doris-regression-hk"
            if (config.s3Source == "aliyun") {
                s3BucketName = "doris-regression-hk"
            } else if (config.s3Source == "aliyun-internal") {
                s3BucketName = "doris-regression-bj"
            } else if (config.s3Source == "tencent") {
                s3BucketName = "doris-build-1308700295"
            } else if (config.s3Source == "huawei") {
                s3BucketName = "doris-build"
            } else if (config.s3Source == "azure") {
                s3BucketName = "qa-build"
            } else if (config.s3Source == "gcp") {
                s3BucketName = "doris-regression"
            }
            config.otherConfigs.put("s3BucketName", "${s3BucketName}")
            log.info("Set s3BucketName to '${s3BucketName}' because not specify.".toString())
        }
        if (config.otherConfigs.get("s3Region") == null) {
            def s3Region = "oss-cn-hongkong"
            if (config.s3Source == "aliyun") {
                s3Region = "oss-cn-hongkong"
            } else if (config.s3Source == "aliyun-internal") {
                s3Region = "oss-cn-beijing"
            } else if (config.s3Source == "tencent") {
                s3Region = "ap-beijing"
            } else if (config.s3Source == "huawei") {
                s3Region = "cn-north-4"
            } else if (config.s3Source == "azure") {
                s3Region = "azure-region"
            } else if (config.s3Source == "gcp") {
                s3Region = "us-central1"
            }
            config.otherConfigs.put("s3Region", "${s3Region}")
            log.info("Set s3Region to '${s3Region}' because not specify.".toString())
        }
        if (config.otherConfigs.get("s3Endpoint") == null) {
            def s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
            if (config.s3Source == "aliyun") {
                s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
            } else if (config.s3Source == "aliyun-internal") {
                s3Endpoint = "oss-cn-beijing-internal.aliyuncs.com"
            } else if (config.s3Source == "tencent") {
                s3Endpoint = "cos.ap-beijing.myqcloud.com"
            } else if (config.s3Source == "huawei") {
                s3Endpoint = "obs.cn-north-4.myhuaweicloud.com"
            } else if (config.s3Source == "azure") {
                s3Endpoint = "azure-endpoint"
            } else if (config.s3Source == "gcp") {
                s3Endpoint = "storage.googleapis.com"
            }
            config.otherConfigs.put("s3Endpoint", "${s3Endpoint}")
            log.info("Set s3Endpoint to '${s3Endpoint}' because not specify.".toString())
        }

        if (config.caseNamePrefix == null) {
            config.caseNamePrefix = ""
            log.info("set caseNamePrefix to '' because not specify.".toString())
        }

        if (config.validateBackupPrefix == null) {
            config.validateBackupPrefix = "doris_validate_backup"
            log.info("set validateBackupPrefix to 'doris_validate_backup' because not specify.".toString())
        }

        if (config.defaultDb == null) {
            config.defaultDb = "regression_test"
            log.info("Set defaultDb to '${config.defaultDb}' because not specify.".toString())
        }

        if (config.jdbcUrl == null) {
            //jdbcUrl needs parameter here. Refer to function: buildUrlWithDb(String jdbcUrl, String dbName)
            config.jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
            log.info("Set jdbcUrl to '${config.jdbcUrl}' because not specify.".toString())
        }

        if (config.jdbcUser == null) {
            config.jdbcUser = "root"
            log.info("Set jdbcUser to '${config.jdbcUser}' because not specify.".toString())
        }

        if (config.jdbcPassword == null) {
            config.jdbcPassword = ""
            log.info("Set jdbcPassword to empty because not specify.".toString())
        }

        if (config.feSourceThriftAddress == null) {
            config.feSourceThriftAddress = "127.0.0.1:9020"
            log.info("Set feThriftAddress to '${config.feSourceThriftAddress}' because not specify.".toString())
        }

        if (config.feTargetThriftAddress == null) {
            config.feTargetThriftAddress = "127.0.0.1:9020"
            log.info("Set feThriftAddress to '${config.feTargetThriftAddress}' because not specify.".toString())
        }

        if (config.feHttpAddress == null) {
            config.feHttpAddress = "127.0.0.1:8030"
            log.info("Set feHttpAddress to '${config.feHttpAddress}' because not specify.".toString())
        }

        if (config.instanceId == null) {
            config.instanceId = "instance_xxx"
            log.info("Set instanceId to '${config.instanceId}' because not specify.".toString())
        }

        if (config.cloudUniqueId == null) {
            config.cloudUniqueId = "cloud_unique_id_xxx"
            log.info("Set cloudUniqueId to '${config.cloudUniqueId}' because not specify.".toString())
        }

        if (config.metaServiceHttpAddress == null) {
            config.metaServiceHttpAddress = "127.0.0.1:5000"
            log.info("Set metaServiceHttpAddress to '${config.metaServiceHttpAddress}' because not specify.".toString())
        }

        if (config.recycleServiceHttpAddress == null) {
            config.recycleServiceHttpAddress = "127.0.0.1:5001"
            log.info("Set recycleServiceHttpAddress to '${config.recycleServiceHttpAddress}' because not specify.".toString())
        }

        if (config.feSyncerUser == null) {
            config.feSyncerUser = "root"
            log.info("Set feSyncerUser to '${config.feSyncerUser}' because not specify.".toString())
        }

        if (config.feSyncerPassword == null) {
            config.feSyncerPassword = ""
            log.info("Set feSyncerPassword to empty because not specify.".toString())
        }

        if (config.syncerAddress == null) {
            config.syncerAddress = "127.0.0.1:9190"
            log.info("Set syncerAddress to '${config.syncerAddress}' because not specify.".toString())
        }

        if (config.feHttpUser == null) {
            config.feHttpUser = "root"
            log.info("Set feHttpUser to '${config.feHttpUser}' because not specify.".toString())
        }

        if (config.feHttpPassword == null) {
            config.feHttpPassword = ""
            log.info("Set feHttpPassword to empty because not specify.".toString())
        }


        if (config.feCloudHttpAddress == null) {
            config.feCloudHttpAddress = "127.0.0.1:8876"
            log.info("Set feCloudHttpAddress to '${config.feCloudHttpAddress}' because not specify.".toString())
        }

        if (config.feCloudHttpUser == null) {
            config.feCloudHttpUser = "root"
            log.info("Set feCloudHttpUser to '${config.feCloudHttpUser}' because not specify.".toString())
        }

        if (config.feCloudHttpPassword == null) {
            config.feCloudHttpPassword = ""
            log.info("Set feCloudHttpPassword to empty because not specify.".toString())
        }

        if (config.suitePath == null) {
            config.suitePath = "regression-test/suites"
            log.info("Set suitePath to '${config.suitePath}' because not specify.".toString())
        }

        if (config.dataPath == null) {
            config.dataPath = "regression-test/data"
            log.info("Set dataPath to '${config.dataPath}' because not specify.".toString())
        }

        if (config.realDataPath == null) {
            config.realDataPath = "regression-test/realData"
            log.info("Set realDataPath to '${config.realDataPath}' because not specify.".toString())
        }

        if (config.cacheDataPath == null) {
            config.cacheDataPath = "regression-test/cacheData"
            log.info("Set cacheDataPath to '${config.cacheDataPath}' because not specify.".toString())
        }

        if (config.enableCacheData == null) {
            config.enableCacheData = true
            log.info("Set enableCacheData to '${config.enableCacheData}' because not specify.".toString())
        }

        if (config.pluginPath == null) {
            config.pluginPath = "regression-test/plugins"
            log.info("Set pluginPath to '${config.pluginPath}' because not specify.".toString())
        }

        if (config.sslCertificatePath == null) {
            config.sslCertificatePath = "regression-test/ssl_default_certificate"
            log.info("Set sslCertificatePath to '${config.sslCertificatePath}' because not specify.".toString())
        }

        if (config.dockerEndDeleteFiles == null) {
            config.dockerEndDeleteFiles = false
            log.info("Set dockerEndDeleteFiles to '${config.dockerEndDeleteFiles}' because not specify.".toString())
        }

        if (config.excludeDockerTest == null) {
            config.excludeDockerTest = true
            log.info("Set excludeDockerTest to '${config.excludeDockerTest}' because not specify.".toString())
        }

        if (config.dorisComposePath == null) {
            config.dorisComposePath = "docker/runtime/doris-compose/doris-compose.py"
            log.info("Set dorisComposePath to '${config.dorisComposePath}' because not specify.".toString())
        }

        if (config.testGroups == null) {
            if (config.isSmokeTest){
                config.testGroups = "smoke"
            } else {
                config.testGroups = "default"
            }
            log.info("Set testGroups to '${config.testGroups}' because not specify.".toString())
        }

        if (config.excludeGroups == null) {
            config.excludeGroups = ""
            log.info("Set excludeGroups to empty because not specify.".toString())
        }

        if (config.testDirectories == null) {
            config.testDirectories = ""
            log.info("Set testDirectories to empty because not specify.".toString())
        }

        if (config.excludeDirectories == null) {
            config.excludeDirectories = ""
            log.info("Set excludeDirectories to empty because not specify.".toString())
        }

        if (config.testSuites == null) {
            config.testSuites = ""
            log.info("Set testSuites to empty because not specify.".toString())
        }

        if (config.excludeSuites == null) {
            config.excludeSuites = ""
            log.info("Set excludeSuites to empty because not specify.".toString())
        }

        if (config.parallel == null) {
            config.parallel = 1
            log.info("Set parallel to 1 because not specify.".toString())
        }

        if (config.suiteParallel == null) {
            config.suiteParallel = 1
            log.info("Set suiteParallel to 1 because not specify.".toString())
        }

        if (config.dockerSuiteParallel == null) {
            config.dockerSuiteParallel = 1
            log.info("Set dockerSuiteParallel to 1 because not specify.".toString())
        }

        if (config.actionParallel == null) {
            config.actionParallel = 10
            log.info("Set actionParallel to 10 because not specify.".toString())
        }
    }

    static String configToString(Object obj) {
        return (obj instanceof String || obj instanceof GString) ? obj.toString() : null
    }

    static Boolean configToBoolean(Object obj) {
        if (obj instanceof Boolean) {
            return (Boolean) obj
        } else if (obj instanceof String || obj instanceof GString) {
            String stringValue = obj.toString().trim()
            if (stringValue.equalsIgnoreCase("true")) {
                return true
            } else if (stringValue.equalsIgnoreCase("false")) {
                return false
            }
        }
        return null
    }

    void createDefaultDb() {
        String dbName = null
        try {
            tryCreateDbIfNotExist(defaultDb)
            dbName = defaultDb
        } catch (Exception e) {
            // defaultDb is not need for most cases.
            // when run docker suites without external fe/be,  createDefaultDb will fail, but can ignore this exception.
            // Infact, only mainly auth_xxx cases use defaultDb, and they just use jdbcUrl in connect function.
            // And they can avoid using defaultDb too. But modify all these cases take a lot work.
            // We better delete all the usage of defaultDb in suites later, and all suites should use their own db, not the defaultDb.
            log.warn("create default db failed ${defaultDb}".toString())
        }

        jdbcUrl = buildUrlWithDb(jdbcUrl, dbName)
        log.info("Reset jdbcUrl to ${jdbcUrl}".toString())
    }

    void tryCreateDbIfNotExist(String dbName) {
        // connect without specify default db
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS ${dbName}"
            log.info("Try to create db, sql: ${sql}".toString())
            if (!dryRun) {
                getConnection().withCloseable { conn ->
                    JdbcUtils.executeToList(conn, sql)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Create database failed, jdbcUrl: ${jdbcUrl}", t)
        }
    }

    void tryCreateDownstreamDbIfNotExist(String dbName = defaultDb) {
        // connect without specify default db
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS ${dbName}"
            log.info("Try to create db, sql: ${sql}".toString())
            if (!dryRun) {
                getDownstreamConnection().withCloseable { conn ->
                    JdbcUtils.executeToList(conn, sql)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Create database failed, ccrDownstreamUrl: ${ccrDownstreamUrl}", t)
        }
    }

    boolean isCloudMode() {
        fetchCloudMode()
        return runMode == RunMode.CLOUD
    }

    void fetchCloudMode() {
        if (runMode == RunMode.UNKNOWN) {
            try {
                def result = JdbcUtils.executeToMapArray(getRootConnection(), "SHOW FRONTEND CONFIG LIKE 'cloud_unique_id'")
                runMode = result[0].Value.toString().isEmpty() ? RunMode.NOT_CLOUD : RunMode.CLOUD
            } catch (Throwable t) {
                throw new IllegalStateException("Fetch server config 'cloud_unique_id' failed, jdbcUrl: ${jdbcUrl}", t)
            }
        }
    }

    boolean isClusterKeyEnabled() {
        try {
            def result = JdbcUtils.executeToMapArray(getRootConnection(), "SHOW FRONTEND CONFIG LIKE 'random_add_cluster_keys_for_mow'")
            log.info("show random_add_cluster_keys_for_mow config: ${result}".toString())
            return result[0].Value.toString().equalsIgnoreCase("true")
        } catch (Throwable t) {
            log.warn("Fetch server config 'random_add_cluster_keys_for_mow' failed, jdbcUrl: ${jdbcUrl}".toString(), t)
            return false
        }
    }

    void excludeUnsupportedCase() {
        boolean isCKEnabled = isClusterKeyEnabled()
        log.info("random_add_cluster_keys_for_mow in fe.conf: ${isCKEnabled}".toString())
        if (isCKEnabled) {
            excludeDirectorySet.add("unique_with_mow_p0/partial_update")
            excludeDirectorySet.add("unique_with_mow_p0/flexible")
            excludeDirectorySet.add("fault_injection_p0/partial_update")
            excludeDirectorySet.add("fault_injection_p0/flexible")
            excludeDirectorySet.add("doc")
            excludeDirectorySet.add("schema_change")
            excludeDirectorySet.add("schema_change_p0")
            excludeDirectorySet.add("schema_change_p2")
            List<String> excludeCases = ["test_table_properties", "test_create_table"
                , "test_default_hll", "test_default_pi", "test_default_bitmap_empty"
                , "test_full_compaction", "test_full_compaction_by_table_id"
                // schema change
                , "test_alter_muti_modify_column"
                // partial update
                , "txn_insert", "test_update_schema_change", "test_generated_column_update", "test_nested_type_with_rowstore", "test_partial_update_generated_column", "nereids_partial_update_native_insert_stmt"
                , "partial_update", "nereids_update_on_current_timestamp", "update_on_current_timestamp", "nereids_delete_mow_partial_update", "delete_mow_partial_update", "test_unique_table_auto_inc"
                , "test_unique_table_auto_inc_partial_update_correct_insert", "partial_update_seq_col", "nereids_partial_update_native_insert_stmt_complex", "regression_test_variant_delete_and_update"
                , "test_unique_table_auto_inc_partial_update_correct_stream_load", "test_update_mow", "test_new_update", "test_update_unique", "nereids_partial_update_native_insert_seq_col"
                , "test_partial_update_rowset_not_found_fault_injection", "test_primary_key_partial_update_broker_load"]
            for (def excludeCase in excludeCases) {
                excludeSuiteWildcard.add(excludeCase)
            }
            log.info("excludeDirectorySet: ${excludeDirectorySet}".toString())
            log.info("excludeSuiteWildcard: ${excludeSuiteWildcard}".toString())
        }
    }

    Connection getConnection() {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    }

    Connection getRootConnection() {
        return DriverManager.getConnection(jdbcUrl, 'root', '')
    }

    Connection getConnectionByArrowFlightSqlDbName(String dbName) {
        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
        String arrowFlightSqlHost = otherConfigs.get("extArrowFlightSqlHost")
        String arrowFlightSqlPort = otherConfigs.get("extArrowFlightSqlPort")
        String arrowFlightSqlUrl = "jdbc:arrow-flight-sql://${arrowFlightSqlHost}:${arrowFlightSqlPort}" +
                "/?useServerPrepStmts=false&useSSL=false&useEncryption=false"
        // Arrow 17.0.0-rc03 support jdbc:arrow-flight-sql connect db
        // https://github.com/apache/arrow/issues/41947
        if (dbName?.trim()) {
            arrowFlightSqlUrl = "jdbc:arrow-flight-sql://${arrowFlightSqlHost}:${arrowFlightSqlPort}" +
                "/catalog=" + dbName + "?useServerPrepStmts=false&useSSL=false&useEncryption=false"
        }
        tryCreateDbIfNotExist(dbName)
        log.info("connect to ${arrowFlightSqlUrl}".toString())
        String arrowFlightSqlJdbcUser = otherConfigs.get("extArrowFlightSqlUser")
        String arrowFlightSqlJdbcPassword = otherConfigs.get("extArrowFlightSqlPassword")
        return DriverManager.getConnection(arrowFlightSqlUrl, arrowFlightSqlJdbcUser, arrowFlightSqlJdbcPassword)
    }

    Connection getDownstreamConnection() {
        return DriverManager.getConnection(ccrDownstreamUrl, ccrDownstreamUser, ccrDownstreamPassword)
    }

    Connection getDownstreamConnectionByDbName(String dbName) {
        log.info("get downstream connection, url: ${ccrDownstreamUrl}, db: ${dbName}, " +
                "user: ${ccrDownstreamUser}, passwd: ${ccrDownstreamPassword}")
        String dbUrl = buildUrlWithDb(ccrDownstreamUrl, dbName)
        tryCreateDownstreamDbIfNotExist(dbName)
        log.info("connect to ${dbUrl}".toString())
        return DriverManager.getConnection(dbUrl, ccrDownstreamUser, ccrDownstreamPassword)
    }

    String getDbNameByFile(File suiteFile) {
        String dir = new File(suitePath).relativePath(suiteFile.parentFile)
        // We put sql files under sql dir, so dbs and tables used by cases
        // under sql directory should be prepared by load.groovy under the
        // parent.
        //
        // e.g.
        // suites/tpcds_sf1/load.groovy
        // suites/tpcds_sf1/sql/q01.sql
        // suites/tpcds_sf1/sql/dir/q01.sql
        if (dir.indexOf(File.separator + "sql", dir.length() - 4) > 0 && dir.endsWith("sql")) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql", dir.length() - 4))
        }
        if (dir.indexOf(File.separator + "sql" + File.separator) > 0) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql" + File.separator))
        }

        dir = dir.replace('-', '_')
        dir = dir.replace('.', '_')

        return defaultDb + '_' + dir.replace(File.separator, '_')
    }

    Predicate<String> getDirectoryFilter() {
        return (Predicate<String>) { String directoryName ->
            if (directories.isEmpty() && excludeDirectorySet.isEmpty()) {
                return true
            }

            String relativePath = new File(suitePath).relativePath(new File(directoryName))
            List<String> allLevelPaths = new ArrayList<>()
            String parentPath = ""
            for (String pathName : relativePath.split(File.separator)) {
                String currentPath = parentPath + pathName
                allLevelPaths.add(currentPath)
                parentPath = currentPath + File.separator
            }

            if (!directories.isEmpty() && !allLevelPaths.any({directories.contains(it) })) {
                return false
            }
            if (!excludeDirectorySet.isEmpty() && allLevelPaths.any({ excludeDirectorySet.contains(it) })) {
                return false
            }
            return true
        }
    }

    public static String buildUrlWithDbImpl(String jdbcUrl, String dbName) {
        if (!dbName?.trim()) {
            return jdbcUrl
        }

        String urlWithDb = jdbcUrl
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        if (urlWithoutSchema.indexOf("/") >= 0) {
            if (jdbcUrl.contains("?")) {
                // e.g: jdbc:mysql://locahost:8080/?a=b
                urlWithDb = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("?"))
                urlWithDb = urlWithDb.substring(0, urlWithDb.lastIndexOf("/"))
                urlWithDb += ("/" + dbName) + jdbcUrl.substring(jdbcUrl.lastIndexOf("?"))
            } else {
                // e.g: jdbc:mysql://locahost:8080/
                urlWithDb += dbName
            }
        } else {
            // e.g: jdbc:mysql://locahost:8080
            urlWithDb += ("/" + dbName)
        }

        return urlWithDb
    }

    public static String buildUrlWithDb(String jdbcUrl, String dbName) {
        String urlWithDb = buildUrlWithDbImpl(jdbcUrl, dbName);
        urlWithDb = addSslUrl(urlWithDb);
        urlWithDb = addTimeoutUrl(urlWithDb);

        return urlWithDb
    }

    public static String buildUrlWithDb(String host, int queryPort, String dbName) {
        def url = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            host, queryPort)
        url = buildUrlWithDb(url, dbName)
        return url
    }

    private static String addSslUrl(String url) {
        if (url.contains("TLS")) {
            return url
        }
        // ssl-mode = PREFERRED
        String useSsl = "true"
        String useSslConfig = "verifyServerCertificate=false&useSSL=" + useSsl + "&requireSSL=false"
        String tlsVersion = "TLSv1.2"
        String tlsVersionConfig = "&enabledTLSProtocols=" + tlsVersion
        String sslUrl = useSslConfig + tlsVersionConfig
        // e.g: jdbc:mysql://locahost:8080/dbname?
        if (url.charAt(url.length() - 1) == '?') {
            return url + sslUrl
            // e.g: jdbc:mysql://locahost:8080/dbname?a=b
        } else if (url.contains('?')) {
            return url + '&' + sslUrl
            // e.g: jdbc:mysql://locahost:8080/dbname
        } else {
            return url + '?' + sslUrl
        }
    }

    private static String addTimeoutUrl(String url) {
        if (url.contains("connectTimeout=") || url.contains("socketTimeout="))
        {
            return url
        }

        Integer connectTimeout = 5000
        Integer socketTimeout = 1000 * 60 * 30
        String s = String.format("connectTimeout=%d&socketTimeout=%d", connectTimeout, socketTimeout)
        if (url.charAt(url.length() - 1) == '?') {
            return url + s
            // e.g: jdbc:mysql://locahost:8080/dbname?a=b
        } else if (url.contains('?')) {
            return url + '&' + s
            // e.g: jdbc:mysql://locahost:8080/dbname
        } else {
            return url + '?' + s
        }
    }
}
