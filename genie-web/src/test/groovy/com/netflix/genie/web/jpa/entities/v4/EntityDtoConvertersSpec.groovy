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
package com.netflix.genie.web.jpa.entities.v4

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ApplicationStatus
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.suppliers.RandomSuppliers
import com.netflix.genie.web.jpa.entities.*
import spock.lang.Specification
/**
 * Specifications for {@link EntityDtoConverters}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class EntityDtoConvertersSpec extends Specification {

    def "Can convert application entity to application v4 dto"() {
        def entity = new ApplicationEntity()
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<TagEntity> tagEntities = tags.collect(
                {
                    new TagEntity(it)
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<FileEntity> configEntities = configs.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setConfigs(configEntities)
        def setupFile = UUID.randomUUID().toString()
        def setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setDependencies(dependencyEntities)
        entity.setStatus(ApplicationStatus.ACTIVE)

        when:
        def application = EntityDtoConverters.toV4ApplicationDto(entity)

        then:
        application.getId() == id
        application.getMetadata().getName() == name
        application.getMetadata().getUser() == user
        application.getMetadata().getVersion().orElseGet(RandomSuppliers.STRING) == version
        application.getCreated() == created
        application.getUpdated() == updated
        application.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        application.getMetadata().getTags() == tags
        application.getResources().getConfigs() == configs
        application.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        application.getResources().getDependencies() == dependencies
        application.getMetadata().getStatus() == ApplicationStatus.ACTIVE
        application.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(application.getMetadata().getMetadata().get()) == metadata
    }

    def "Can convert cluster entity to v4 cluster dto"() {
        def entity = new ClusterEntity()
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        entity.setStatus(ClusterStatus.TERMINATED)
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> configEntities = configs.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setConfigs(configEntities)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setDependencies(dependencyEntities)
        def setupFile = UUID.randomUUID().toString()
        entity.setSetupFile(new FileEntity(setupFile))

        when:
        def cluster = EntityDtoConverters.toV4ClusterDto(entity)

        then:
        cluster.getId() == id
        cluster.getMetadata().getName() == name
        cluster.getMetadata().getUser() == user
        cluster.getMetadata().getVersion().orElseGet(RandomSuppliers.STRING) == version
        cluster.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        cluster.getMetadata().getStatus() == ClusterStatus.TERMINATED
        cluster.getCreated() == created
        cluster.getUpdated() == updated
        cluster.getMetadata().getTags() == tags
        cluster.getResources().getConfigs() == configs
        cluster.getResources().getDependencies() == dependencies
        cluster.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        cluster.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(cluster.getMetadata().getMetadata().get()) == metadata
    }

    def "Can convert command entity to v4 command dto"() {
        def entity = new CommandEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        entity.setStatus(CommandStatus.DEPRECATED)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    new TagEntity(it)
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> configEntities = configs.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setConfigs(configEntities)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setDependencies(dependencyEntities)
        def setupFile = UUID.randomUUID().toString()
        final FileEntity setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)
        def executable = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        entity.setExecutable(executable)
        def checkDelay = 2180234L
        entity.setCheckDelay(checkDelay)
        def memory = 10_241
        entity.setMemory(memory)

        when:
        def command = EntityDtoConverters.toV4CommandDto(entity)

        then:
        command.getId() == id
        command.getMetadata().getName() == name
        command.getMetadata().getUser() == user
        command.getMetadata().getVersion().orElseGet(RandomSuppliers.STRING) == version
        command.getMetadata().getStatus() == CommandStatus.DEPRECATED
        command.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        command.getCreated() == created
        command.getUpdated() == updated
        command.getExecutable() == executable
        command.getMetadata().getTags() == tags
        command.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        command.getResources().getConfigs() == configs
        command.getResources().getDependencies() == dependencies
        command.getMemory().orElseGet(RandomSuppliers.INT) == memory
        command.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(command.getMetadata().getMetadata().get()) == metadata
        command.getCheckDelay() == checkDelay
    }
}
