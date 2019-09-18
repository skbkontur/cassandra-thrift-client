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

  #if !SILVERLIGHT
  [Serializable]
  #endif
  public partial class CqlPreparedResult : TBase
  {
    private List<string> _variable_types;
    private List<string> _variable_names;

    public int ItemId { get; set; }

    public int Count { get; set; }

    public List<string> Variable_types
    {
      get
      {
        return _variable_types;
      }
      set
      {
        __isset.variable_types = true;
        this._variable_types = value;
      }
    }

    public List<string> Variable_names
    {
      get
      {
        return _variable_names;
      }
      set
      {
        __isset.variable_names = true;
        this._variable_names = value;
      }
    }


    public Isset __isset;
    #if !SILVERLIGHT
    [Serializable]
    #endif
    public struct Isset {
      public bool variable_types;
      public bool variable_names;
    }

    public CqlPreparedResult() {
    }

    public CqlPreparedResult(int itemId, int count) : this() {
      this.ItemId = itemId;
      this.Count = count;
    }

    public void Read (TProtocol iprot)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_itemId = false;
        bool isset_count = false;
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
              if (field.Type == TType.I32) {
                ItemId = iprot.ReadI32();
                isset_itemId = true;
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 2:
              if (field.Type == TType.I32) {
                Count = iprot.ReadI32();
                isset_count = true;
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 3:
              if (field.Type == TType.List) {
                {
                  Variable_types = new List<string>();
                  TList _list100 = iprot.ReadListBegin();
                  for( int _i101 = 0; _i101 < _list100.Count; ++_i101)
                  {
                    string _elem102;
                    _elem102 = iprot.ReadString();
                    Variable_types.Add(_elem102);
                  }
                  iprot.ReadListEnd();
                }
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 4:
              if (field.Type == TType.List) {
                {
                  Variable_names = new List<string>();
                  TList _list103 = iprot.ReadListBegin();
                  for( int _i104 = 0; _i104 < _list103.Count; ++_i104)
                  {
                    string _elem105;
                    _elem105 = iprot.ReadString();
                    Variable_names.Add(_elem105);
                  }
                  iprot.ReadListEnd();
                }
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
        if (!isset_itemId)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field ItemId not set");
        if (!isset_count)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Count not set");
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
        TStruct struc = new TStruct("CqlPreparedResult");
        oprot.WriteStructBegin(struc);
        TField field = new TField();
        field.Name = "itemId";
        field.Type = TType.I32;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteI32(ItemId);
        oprot.WriteFieldEnd();
        field.Name = "count";
        field.Type = TType.I32;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteI32(Count);
        oprot.WriteFieldEnd();
        if (Variable_types != null && __isset.variable_types) {
          field.Name = "variable_types";
          field.Type = TType.List;
          field.ID = 3;
          oprot.WriteFieldBegin(field);
          {
            oprot.WriteListBegin(new TList(TType.String, Variable_types.Count));
            foreach (string _iter106 in Variable_types)
            {
              oprot.WriteString(_iter106);
            }
            oprot.WriteListEnd();
          }
          oprot.WriteFieldEnd();
        }
        if (Variable_names != null && __isset.variable_names) {
          field.Name = "variable_names";
          field.Type = TType.List;
          field.ID = 4;
          oprot.WriteFieldBegin(field);
          {
            oprot.WriteListBegin(new TList(TType.String, Variable_names.Count));
            foreach (string _iter107 in Variable_names)
            {
              oprot.WriteString(_iter107);
            }
            oprot.WriteListEnd();
          }
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
      StringBuilder __sb = new StringBuilder("CqlPreparedResult(");
      __sb.Append(", ItemId: ");
      __sb.Append(ItemId);
      __sb.Append(", Count: ");
      __sb.Append(Count);
      if (Variable_types != null && __isset.variable_types) {
        __sb.Append(", Variable_types: ");
        __sb.Append(Variable_types);
      }
      if (Variable_names != null && __isset.variable_names) {
        __sb.Append(", Variable_names: ");
        __sb.Append(Variable_names);
      }
      __sb.Append(")");
      return __sb.ToString();
    }

  }

}
