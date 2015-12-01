/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package fr.finaxys.tutorials.utils.avro.models;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class AgentReferential extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AgentReferential\",\"namespace\":\"fr.finaxys.tutorials.utils.avro.models\",\"fields\":[{\"name\":\"Trace\",\"type\":\"string\"},{\"name\":\"AgentRefId\",\"type\":\"int\"},{\"name\":\"AgentName\",\"type\":\"string\"},{\"name\":\"IsMarketMaker\",\"type\":\"string\"},{\"name\":\"Details\",\"type\":\"string\"},{\"name\":\"Timestamp\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence Trace;
  @Deprecated public int AgentRefId;
  @Deprecated public java.lang.CharSequence AgentName;
  @Deprecated public java.lang.CharSequence IsMarketMaker;
  @Deprecated public java.lang.CharSequence Details;
  @Deprecated public long Timestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public AgentReferential() {}

  /**
   * All-args constructor.
   */
  public AgentReferential(java.lang.CharSequence Trace, java.lang.Integer AgentRefId, java.lang.CharSequence AgentName, java.lang.CharSequence IsMarketMaker, java.lang.CharSequence Details, java.lang.Long Timestamp) {
    this.Trace = Trace;
    this.AgentRefId = AgentRefId;
    this.AgentName = AgentName;
    this.IsMarketMaker = IsMarketMaker;
    this.Details = Details;
    this.Timestamp = Timestamp;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return Trace;
    case 1: return AgentRefId;
    case 2: return AgentName;
    case 3: return IsMarketMaker;
    case 4: return Details;
    case 5: return Timestamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: Trace = (java.lang.CharSequence)value$; break;
    case 1: AgentRefId = (java.lang.Integer)value$; break;
    case 2: AgentName = (java.lang.CharSequence)value$; break;
    case 3: IsMarketMaker = (java.lang.CharSequence)value$; break;
    case 4: Details = (java.lang.CharSequence)value$; break;
    case 5: Timestamp = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'Trace' field.
   */
  public java.lang.CharSequence getTrace() {
    return Trace;
  }

  /**
   * Sets the value of the 'Trace' field.
   * @param value the value to set.
   */
  public void setTrace(java.lang.CharSequence value) {
    this.Trace = value;
  }

  /**
   * Gets the value of the 'AgentRefId' field.
   */
  public java.lang.Integer getAgentRefId() {
    return AgentRefId;
  }

  /**
   * Sets the value of the 'AgentRefId' field.
   * @param value the value to set.
   */
  public void setAgentRefId(java.lang.Integer value) {
    this.AgentRefId = value;
  }

  /**
   * Gets the value of the 'AgentName' field.
   */
  public java.lang.CharSequence getAgentName() {
    return AgentName;
  }

  /**
   * Sets the value of the 'AgentName' field.
   * @param value the value to set.
   */
  public void setAgentName(java.lang.CharSequence value) {
    this.AgentName = value;
  }

  /**
   * Gets the value of the 'IsMarketMaker' field.
   */
  public java.lang.CharSequence getIsMarketMaker() {
    return IsMarketMaker;
  }

  /**
   * Sets the value of the 'IsMarketMaker' field.
   * @param value the value to set.
   */
  public void setIsMarketMaker(java.lang.CharSequence value) {
    this.IsMarketMaker = value;
  }

  /**
   * Gets the value of the 'Details' field.
   */
  public java.lang.CharSequence getDetails() {
    return Details;
  }

  /**
   * Sets the value of the 'Details' field.
   * @param value the value to set.
   */
  public void setDetails(java.lang.CharSequence value) {
    this.Details = value;
  }

  /**
   * Gets the value of the 'Timestamp' field.
   */
  public java.lang.Long getTimestamp() {
    return Timestamp;
  }

  /**
   * Sets the value of the 'Timestamp' field.
   * @param value the value to set.
   */
  public void setTimestamp(java.lang.Long value) {
    this.Timestamp = value;
  }

  /** Creates a new AgentReferential RecordBuilder */
  public static fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder newBuilder() {
    return new fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder();
  }
  
  /** Creates a new AgentReferential RecordBuilder by copying an existing Builder */
  public static fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder newBuilder(fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder other) {
    return new fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder(other);
  }
  
  /** Creates a new AgentReferential RecordBuilder by copying an existing AgentReferential instance */
  public static fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder newBuilder(fr.finaxys.tutorials.utils.avro.models.AgentReferential other) {
    return new fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder(other);
  }
  
  /**
   * RecordBuilder for AgentReferential instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AgentReferential>
    implements org.apache.avro.data.RecordBuilder<AgentReferential> {

    private java.lang.CharSequence Trace;
    private int AgentRefId;
    private java.lang.CharSequence AgentName;
    private java.lang.CharSequence IsMarketMaker;
    private java.lang.CharSequence Details;
    private long Timestamp;

    /** Creates a new Builder */
    private Builder() {
      super(fr.finaxys.tutorials.utils.avro.models.AgentReferential.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.Trace)) {
        this.Trace = data().deepCopy(fields()[0].schema(), other.Trace);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.AgentRefId)) {
        this.AgentRefId = data().deepCopy(fields()[1].schema(), other.AgentRefId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.AgentName)) {
        this.AgentName = data().deepCopy(fields()[2].schema(), other.AgentName);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.IsMarketMaker)) {
        this.IsMarketMaker = data().deepCopy(fields()[3].schema(), other.IsMarketMaker);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Details)) {
        this.Details = data().deepCopy(fields()[4].schema(), other.Details);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.Timestamp)) {
        this.Timestamp = data().deepCopy(fields()[5].schema(), other.Timestamp);
        fieldSetFlags()[5] = true;
      }
    }
    
    /** Creates a Builder by copying an existing AgentReferential instance */
    private Builder(fr.finaxys.tutorials.utils.avro.models.AgentReferential other) {
            super(fr.finaxys.tutorials.utils.avro.models.AgentReferential.SCHEMA$);
      if (isValidValue(fields()[0], other.Trace)) {
        this.Trace = data().deepCopy(fields()[0].schema(), other.Trace);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.AgentRefId)) {
        this.AgentRefId = data().deepCopy(fields()[1].schema(), other.AgentRefId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.AgentName)) {
        this.AgentName = data().deepCopy(fields()[2].schema(), other.AgentName);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.IsMarketMaker)) {
        this.IsMarketMaker = data().deepCopy(fields()[3].schema(), other.IsMarketMaker);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Details)) {
        this.Details = data().deepCopy(fields()[4].schema(), other.Details);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.Timestamp)) {
        this.Timestamp = data().deepCopy(fields()[5].schema(), other.Timestamp);
        fieldSetFlags()[5] = true;
      }
    }

    /** Gets the value of the 'Trace' field */
    public java.lang.CharSequence getTrace() {
      return Trace;
    }
    
    /** Sets the value of the 'Trace' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setTrace(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.Trace = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'Trace' field has been set */
    public boolean hasTrace() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'Trace' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearTrace() {
      Trace = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'AgentRefId' field */
    public java.lang.Integer getAgentRefId() {
      return AgentRefId;
    }
    
    /** Sets the value of the 'AgentRefId' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setAgentRefId(int value) {
      validate(fields()[1], value);
      this.AgentRefId = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'AgentRefId' field has been set */
    public boolean hasAgentRefId() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'AgentRefId' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearAgentRefId() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'AgentName' field */
    public java.lang.CharSequence getAgentName() {
      return AgentName;
    }
    
    /** Sets the value of the 'AgentName' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setAgentName(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.AgentName = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'AgentName' field has been set */
    public boolean hasAgentName() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'AgentName' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearAgentName() {
      AgentName = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'IsMarketMaker' field */
    public java.lang.CharSequence getIsMarketMaker() {
      return IsMarketMaker;
    }
    
    /** Sets the value of the 'IsMarketMaker' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setIsMarketMaker(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.IsMarketMaker = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'IsMarketMaker' field has been set */
    public boolean hasIsMarketMaker() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'IsMarketMaker' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearIsMarketMaker() {
      IsMarketMaker = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'Details' field */
    public java.lang.CharSequence getDetails() {
      return Details;
    }
    
    /** Sets the value of the 'Details' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setDetails(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.Details = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'Details' field has been set */
    public boolean hasDetails() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'Details' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearDetails() {
      Details = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'Timestamp' field */
    public java.lang.Long getTimestamp() {
      return Timestamp;
    }
    
    /** Sets the value of the 'Timestamp' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder setTimestamp(long value) {
      validate(fields()[5], value);
      this.Timestamp = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'Timestamp' field has been set */
    public boolean hasTimestamp() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'Timestamp' field */
    public fr.finaxys.tutorials.utils.avro.models.AgentReferential.Builder clearTimestamp() {
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    public AgentReferential build() {
      try {
        AgentReferential record = new AgentReferential();
        record.Trace = fieldSetFlags()[0] ? this.Trace : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.AgentRefId = fieldSetFlags()[1] ? this.AgentRefId : (java.lang.Integer) defaultValue(fields()[1]);
        record.AgentName = fieldSetFlags()[2] ? this.AgentName : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.IsMarketMaker = fieldSetFlags()[3] ? this.IsMarketMaker : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.Details = fieldSetFlags()[4] ? this.Details : (java.lang.CharSequence) defaultValue(fields()[4]);
        record.Timestamp = fieldSetFlags()[5] ? this.Timestamp : (java.lang.Long) defaultValue(fields()[5]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
