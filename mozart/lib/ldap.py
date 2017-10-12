import simpleldap, traceback


from mozart import app


def ldap_user_verified(username, password):
    """Verify user via ldap."""

    host = app.config['LDAP_HOST']
    base_dn = app.config['LDAP_BASEDN']
    groups = app.config['LDAP_GROUPS']

    try:
        l = simpleldap.Connection(host, dn='uid=%s,%s' % (username, base_dn),
                                  encryption='ssl', password=password) 
    except Exception, e:
        app.logger.info("Got error trying to verify LDAP user %s:" % username)
        app.logger.info("%s:\n\n%s" % (str(e), traceback.format_exc()))
        return None
   
    # validate user
    r = l.search('uid=%s' % username, base_dn=base_dn)
    if len(r) != 1:
        app.logger.info("Got invalid number of entries for %s: %s" % (username, len(r)))
        app.logger.info("r: %s" % str(r))
        return None

    # validate user is part of a group allowed
    uid = 'uid=%s,%s' % (username, base_dn)
    for group in groups:
        g = l.search('cn=%s' % group, base_dn=base_dn)
        for this_g in g:
            if uid in this_g['uniqueMember']: return dict(r[0])

    app.logger.info("User %s is not part of any approved LDAP groups." % username)
    return None
