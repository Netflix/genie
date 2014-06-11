package com.netflix.genie.server.services.impl.jpa;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Application_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ApplicationConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenJPA based implementation of the ApplicationConfigService.
 *
 * @author amsharma
 * @author tgianos
 */
public class ApplicationConfigServiceJPAImpl implements ApplicationConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigServiceJPAImpl.class);

    private final PersistenceManager<Application> pm;

    /**
     * Default constructor.
     */
    public ApplicationConfigServiceJPAImpl() {
        // instantiate PersistenceManager
        this.pm = new PersistenceManager<Application>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Application getApplicationConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            //Messages will be logged by exception mapper at resource level
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to get");
        }
        LOG.debug("Called with id " + id);
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Application app = em.find(Application.class, id);
            if (app == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "No application with id " + id);
            } else {
                return app;
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getApplicationConfigs(
            final String name,
            final String userName,
            final int page,
            final int limit) {
        LOG.debug("Called");

        final EntityManager em = this.pm.createEntityManager();
        try {
            final CriteriaBuilder cb = em.getCriteriaBuilder();
            final CriteriaQuery<Application> cq = cb.createQuery(Application.class);
            final Root<Application> a = cq.from(Application.class);
            final List<Predicate> predicates = new ArrayList<Predicate>();
            if (StringUtils.isNotEmpty(name)) {
                predicates.add(cb.equal(a.get(Application_.name), name));
            }
            if (StringUtils.isNotEmpty(userName)) {
                predicates.add(cb.equal(a.get(Application_.user), userName));
            }
            cq.where(cb.and(predicates.toArray(new Predicate[0])));
            final TypedQuery<Application> query = em.createQuery(cq);
            final int finalPage = page < 0 ? PersistenceManager.DEFAULT_PAGE_NUMBER : page;
            final int finalLimit = limit < 0 ? PersistenceManager.DEFAULT_PAGE_SIZE : limit;
            query.setMaxResults(finalLimit);
            query.setFirstResult(finalLimit * finalPage);
            return query.getResultList();
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Application createApplicationConfig(final Application app) throws CloudServiceException {
        Application.validate(app);
        LOG.debug("Called with application: " + app.toString());
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            if (em.contains(app)) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "An application with id " + app.getId() + " already exists");
            }
            em.persist(app);
            trans.commit();
            return app;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Application updateApplicationConfig(final String id, final Application updateApp) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update.");
        }
        if (updateApp == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application information entered. Unable to update.");
        }
        LOG.debug("Called with app " + updateApp.toString());
        //TODO: What to do about sets like jars, etc?
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists to update.");
            }
            if (StringUtils.isNotEmpty(updateApp.getEnvPropFile())) {
                app.setEnvPropFile(updateApp.getEnvPropFile());
            }
            if (StringUtils.isNotEmpty(updateApp.getName())) {
                app.setName(updateApp.getName());
            }
            if (StringUtils.isNotEmpty(updateApp.getUser())) {
                app.setUser(updateApp.getUser());
            }
            if (StringUtils.isNotEmpty(updateApp.getVersion())) {
                app.setVersion(updateApp.getVersion());
            }
            if (updateApp.getStatus() != null
                    && updateApp.getStatus() != app.getStatus()) {
                app.setStatus(updateApp.getStatus());
            }
            Application.validate(app);
            trans.commit();
            return app;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Application deleteApplicationConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to delete.");
        }
        LOG.debug("Called with id " + id);
        final EntityManager em = pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
            }
            final Set<Command> commands = app.getCommands();
            if (commands != null) {
                for (final Command command : commands) {
                    final Set<Application> apps = command.getApplications();
                    if (apps != null) {
                        apps.remove(app);
                    }
                }
            }
            em.remove(app);
            trans.commit();
            return app;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }
}
