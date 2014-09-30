package org.pentaho.di.trans.steps.redis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import redis.clients.jedis.HostAndPort;

/**
 * The Redis Output step writes value objects, for the given key names, to Redis server(s).
 */
@Step( id = "RedisOutput", image = "redis-output.png", name = "Redis Output",
      description = "Writes to a Redis instance", categoryDescription = "Output" )
public class RedisOutputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = RedisOutputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private String keyFieldName;
  private String valueFieldName;
  private int expirationTime;
  private Set<HostAndPort> servers;

  public RedisOutputMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    RedisOutputMeta retval = (RedisOutputMeta) super.clone();
    retval.setKeyFieldName( this.keyFieldName );
    retval.setValueFieldName( this.valueFieldName );
    retval.setExpirationTime( this.expirationTime );
    return retval;
  }

  public void allocate( int nrfields ) {
    servers = new HashSet<HostAndPort>();
  }

  public void setDefault() {
    this.keyFieldName = null;
    this.valueFieldName = null;
    this.expirationTime = 0;
  }

  public void getFields( RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    super.getFields( inputRowMeta, origin, info, nextStep, space, repository, metaStore );
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                     String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
                  "RedisOutputMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "RedisOutputMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "RedisOutputMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                  "RedisOutputMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new RedisOutput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new RedisOutputData();
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  public void setKeyFieldName( String keyFieldName ) {
    this.keyFieldName = keyFieldName;
  }

  public String getValueFieldName() {
    return valueFieldName;
  }

  public void setValueFieldName( String valueFieldName ) {
    this.valueFieldName = valueFieldName;
  }

  public int getExpirationTime() {
    return expirationTime;
  }

  public void setExpirationTime( int expirationTime ) {
    this.expirationTime = expirationTime;
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "keyfield", this.getKeyFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "valuefield", this.getValueFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "expiration", this.getExpirationTime() ) );
    retval.append( "    <servers>" ).append( Const.CR );

    Set<HostAndPort> servers = this.getServers();
    if ( servers != null ) {
      for ( HostAndPort addr : servers ) {
        retval.append( "      <server>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "hostname", addr.getHost() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "port", addr.getPort() ) );
        retval.append( "      </server>" ).append( Const.CR );
      }
    }
    retval.append( "    </servers>" ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      this.keyFieldName = XMLHandler.getTagValue( stepnode, "keyfield" );
      this.valueFieldName = XMLHandler.getTagValue( stepnode, "valuefield" );
      this.expirationTime = Integer.parseInt( XMLHandler.getTagValue( stepnode, "expiration" ) );
      Node serverNodes = XMLHandler.getSubNode( stepnode, "servers" );
      int nrservers = XMLHandler.countNodes( serverNodes, "server" );

      allocate( nrservers );

      for ( int i = 0; i < nrservers; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( serverNodes, "server", i );

        String hostname = XMLHandler.getTagValue( fnode, "hostname" );
        int port = Integer.parseInt( XMLHandler.getTagValue( fnode, "port" ) );
        servers.add( new HostAndPort( hostname, port ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "RedisOutputMeta.Exception.UnableToReadStepInfo" ),
            e );
    }
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
        throws KettleException {
    try {
      this.keyFieldName = rep.getStepAttributeString( id_step, "keyfield" );
      this.valueFieldName = rep.getStepAttributeString( id_step, "valuefield" );
      this.expirationTime = (int) rep.getStepAttributeInteger( id_step, "expiration" );

      int nrservers = rep.countNrStepAttributes( id_step, "server" );

      allocate( nrservers );

      for ( int i = 0; i < nrservers; i++ ) {
        servers.add( new HostAndPort( rep.getStepAttributeString( id_step, i, "hostname" ), Integer.parseInt( rep
              .getStepAttributeString( id_step, i, "port" ) ) ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
            "RedisOutputMeta.Exception.UnexpectedErrorReadingStepInfo" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
        throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "keyfield", this.keyFieldName );
      rep.saveStepAttribute( id_transformation, id_step, "valuefield", this.valueFieldName );
      rep.saveStepAttribute( id_transformation, id_step, "expiration", this.expirationTime );
      int i = 0;
      Set<HostAndPort> servers = this.getServers();
      if ( servers != null ) {
        for ( HostAndPort addr : servers ) {
          rep.saveStepAttribute( id_transformation, id_step, i++, "hostname", addr.getHost() );
          rep.saveStepAttribute( id_transformation, id_step, i++, "port", addr.getPort() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
            "RedisOutputMeta.Exception.UnexpectedErrorSavingStepInfo" ), e );
    }
  }

  public Set<HostAndPort> getServers() {
    return servers;
  }

  public void setServers( Set<HostAndPort> servers ) {
    this.servers = servers;
  }

}
