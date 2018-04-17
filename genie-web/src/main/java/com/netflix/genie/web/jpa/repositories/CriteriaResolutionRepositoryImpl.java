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
package com.netflix.genie.web.jpa.repositories;

import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Implementations of the {@link CriteriaResolutionRepository} interface.
 * <p>
 * Works as a fragment.
 * See <a href="https://tinyurl.com/yctelbfh">Spring Data JPA Documentation</a> for more.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class CriteriaResolutionRepositoryImpl implements CriteriaResolutionRepository {

    private static final String CLUSTER_QUERY_STRING = "{CLUSTER_QUERY_HERE}";
    private static final String COMMAND_QUERY_STRING = "{COMMAND_QUERY_HERE}";

    private static final String RESOLVE_CLUSTERS_AND_COMMANDS_QUERY =
        "SELECT "
            + "  cc.cluster_id,"
            + "  c.unique_id "
            + "FROM"
            + "  ("
            + "    SELECT"
            + "      cc.cluster_id as cluster_id,"
            + "      MIN(cc.command_order) as command_order"
            + "    FROM"
            + "      (" + CLUSTER_QUERY_STRING + ") AS selected_clusters join"
            + "      clusters_commands cc ON selected_clusters.id = cc.cluster_id join"
            + "      (" + COMMAND_QUERY_STRING + ") AS selected_commands ON selected_commands.id = cc.command_id"
            + "    GROUP BY cc.cluster_id"
            + "  ) as cluster_id_order join"
            + "  clusters_commands cc on"
            + "    cluster_id_order.cluster_id = cc.cluster_id AND"
            + "    cc.command_order = cluster_id_order.command_order join"
            + "  commands c on cc.command_id = c.id;";

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public List<Object[]> resolveClustersAndCommands(
        final Criterion clusterCriterion,
        final Criterion commandCriterion
    ) {
        return this.entityManager
            .createNativeQuery(
                RESOLVE_CLUSTERS_AND_COMMANDS_QUERY
                    .replace(CLUSTER_QUERY_STRING, this.buildEntityQueryForType(clusterCriterion, CriteriaType.CLUSTER))
                    .replace(COMMAND_QUERY_STRING, this.buildEntityQueryForType(commandCriterion, CriteriaType.COMMAND))
            )
            .getResultList();
    }

    private String buildEntityQueryForType(final Criterion criterion, final CriteriaType criteriaType) {
        final Set<String> tags = criterion.getTags();
        final boolean hasTags = !tags.isEmpty();
        final StringBuilder query = new StringBuilder();
        query
            .append("SELECT c.id as id FROM ")
            .append(criteriaType.getPrimaryTable())
            .append(" c");

        if (hasTags) {
            query
                .append(" join ")
                .append(criteriaType.getTagTable())
                .append(" ct on c.id = ct.")
                .append(criteriaType.getTagJoinColumn())
                .append(" join tags t on ct.tag_id = t.id");
        }

        query.append(" WHERE");

        criterion.getId().ifPresent(
            id -> {
                if (StringUtils.isNotBlank(id)) {
                    query.append(" c.unique_id = '").append(id).append("' AND");
                }
            }
        );
        criterion.getName().ifPresent(
            name -> {
                if (StringUtils.isNotBlank(name)) {
                    query.append(" c.name = '").append(name).append("' AND");
                }
            }
        );
        criterion.getVersion().ifPresent(
            version -> {
                if (StringUtils.isNotBlank(version)) {
                    query.append(" c.version = '").append(version).append("' AND");
                }
            }
        );

        if (hasTags) {
            query
                .append(" t.tag IN (")
                .append(
                    criterion
                        .getTags()
                        .stream()
                        .map(tag -> "'" + tag + "'")
                        .reduce((first, second) -> first + ", " + second)
                        .orElse("")
                )
                .append(") AND");
        }

        final String status;
        final Optional<String> criterionStatus = criterion.getStatus();
        if (criterionStatus.isPresent()) {
            final String unwrappedStatus = criterionStatus.get();
            status = StringUtils.isBlank(unwrappedStatus) ? criteriaType.getDefaultStatus() : unwrappedStatus;
        } else {
            status = criteriaType.getDefaultStatus();
        }
        query
            .append(" c.status = '")
            .append(status)
            .append("'");

        if (hasTags) {
            query
                .append(" GROUP BY c.id HAVING COUNT(c.id) = ")
                .append(criterion.getTags().size());
        }

        return query.toString();
    }


    /**
     * Enumeration of the types of criteria and default values that can be supplied to the cluster and command
     * resolution methods.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    private enum CriteriaType {

        CLUSTER("clusters", "clusters_tags", "cluster_id", ClusterStatus.UP.toString()),

        COMMAND("commands", "commands_tags", "command_id", CommandStatus.ACTIVE.toString());

        private final String primaryTable;
        private final String tagTable;
        private final String tagJoinColumn;
        private final String defaultStatus;

        CriteriaType(
            final String primaryTable,
            final String tagTable,
            final String tagJoinColumn,
            final String defaultStatus
        ) {
            this.primaryTable = primaryTable;
            this.tagTable = tagTable;
            this.tagJoinColumn = tagJoinColumn;
            this.defaultStatus = defaultStatus;
        }
    }
}
