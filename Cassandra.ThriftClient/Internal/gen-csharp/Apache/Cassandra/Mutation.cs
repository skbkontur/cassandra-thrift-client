/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using System.Runtime.Serialization;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Cassandra
{

  /// <summary>
  /// A Mutation is either an insert (represented by filling column_or_supercolumn) or a deletion (represented by filling the deletion attribute).
  /// @param column_or_supercolumn. An insert to a column or supercolumn (possibly counter column or supercolumn)
  /// @param deletion. A deletion of a column or supercolumn
  /// </summary>
  #if !SILVERLIGHT
  [Serializable]
  #endif
  internal partial class Mutation : TBase
  {
    private ColumnOrSuperColumn _column_or_supercolumn;
    private Deletion _deletion;

    public ColumnOrSuperColumn Column_or_supercolumn
    {
      get
      {
        return _column_or_supercolumn;
      }
      set
      {
        __isset.column_or_supercolumn = true;
        this._column_or_supercolumn = value;
      }
    }

    public Deletion Deletion
    {
      get
      {
        return _deletion;
      }
      set
      {
        __isset.deletion = true;
        this._deletion = value;
      }
    }


    public Isset __isset;
    #if !SILVERLIGHT
    [Serializable]
    #endif
    internal struct Isset {
      public bool column_or_supercolumn;
      public bool deletion;
    }

    public Mutation() {
    }

    public void Read (TProtocol iprot)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        TField field;
        iprot.ReadStructBegin();
        while (true)
        {
          field = iprot.ReadFieldBegin();
          if (field.Type == TType.Stop) { 
            break;
          }
          switch (field.ID)
          {
            case 1:
              if (field.Type == TType.Struct) {
                Column_or_supercolumn = new ColumnOrSuperColumn();
                Column_or_supercolumn.Read(iprot);
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 2:
              if (field.Type == TType.Struct) {
                Deletion = new Deletion();
                Deletion.Read(iprot);
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            default: 
              TProtocolUtil.Skip(iprot, field.Type);
              break;
          }
          iprot.ReadFieldEnd();
        }
        iprot.ReadStructEnd();
      }
      finally
      {
        iprot.DecrementRecursionDepth();
      }
    }

    public void Write(TProtocol oprot) {
      oprot.IncrementRecursionDepth();
      try
      {
        TStruct struc = new TStruct("Mutation");
        oprot.WriteStructBegin(struc);
        TField field = new TField();
        if (Column_or_supercolumn != null && __isset.column_or_supercolumn) {
          field.Name = "column_or_supercolumn";
          field.Type = TType.Struct;
          field.ID = 1;
          oprot.WriteFieldBegin(field);
          Column_or_supercolumn.Write(oprot);
          oprot.WriteFieldEnd();
        }
        if (Deletion != null && __isset.deletion) {
          field.Name = "deletion";
          field.Type = TType.Struct;
          field.ID = 2;
          oprot.WriteFieldBegin(field);
          Deletion.Write(oprot);
          oprot.WriteFieldEnd();
        }
        oprot.WriteFieldStop();
        oprot.WriteStructEnd();
      }
      finally
      {
        oprot.DecrementRecursionDepth();
      }
    }

    public override string ToString() {
      StringBuilder __sb = new StringBuilder("Mutation(");
      bool __first = true;
      if (Column_or_supercolumn != null && __isset.column_or_supercolumn) {
        if(!__first) { __sb.Append(", "); }
        __first = false;
        __sb.Append("Column_or_supercolumn: ");
        __sb.Append(Column_or_supercolumn== null ? "<null>" : Column_or_supercolumn.ToString());
      }
      if (Deletion != null && __isset.deletion) {
        if(!__first) { __sb.Append(", "); }
        __first = false;
        __sb.Append("Deletion: ");
        __sb.Append(Deletion== null ? "<null>" : Deletion.ToString());
      }
      __sb.Append(")");
      return __sb.ToString();
    }

  }

}
