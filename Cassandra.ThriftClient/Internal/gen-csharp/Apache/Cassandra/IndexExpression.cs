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
  internal partial class IndexExpression : TBase
  {

    public byte[] Column_name { get; set; }

    /// <summary>
    /// 
    /// <seealso cref="IndexOperator"/>
    /// </summary>
    public IndexOperator Op { get; set; }

    public byte[] Value { get; set; }

    public IndexExpression() {
    }

    public IndexExpression(byte[] column_name, IndexOperator op, byte[] @value) : this() {
      this.Column_name = column_name;
      this.Op = op;
      this.Value = @value;
    }

    public void Read (TProtocol iprot)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_column_name = false;
        bool isset_op = false;
        bool isset_value = false;
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
              if (field.Type == TType.String) {
                Column_name = iprot.ReadBinary();
                isset_column_name = true;
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 2:
              if (field.Type == TType.I32) {
                Op = (IndexOperator)iprot.ReadI32();
                isset_op = true;
              } else { 
                TProtocolUtil.Skip(iprot, field.Type);
              }
              break;
            case 3:
              if (field.Type == TType.String) {
                Value = iprot.ReadBinary();
                isset_value = true;
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
        if (!isset_column_name)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Column_name not set");
        if (!isset_op)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Op not set");
        if (!isset_value)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Value not set");
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
        TStruct struc = new TStruct("IndexExpression");
        oprot.WriteStructBegin(struc);
        TField field = new TField();
        if (Column_name == null)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Column_name not set");
        field.Name = "column_name";
        field.Type = TType.String;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteBinary(Column_name);
        oprot.WriteFieldEnd();
        field.Name = "op";
        field.Type = TType.I32;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteI32((int)Op);
        oprot.WriteFieldEnd();
        if (Value == null)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Value not set");
        field.Name = "value";
        field.Type = TType.String;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        oprot.WriteBinary(Value);
        oprot.WriteFieldEnd();
        oprot.WriteFieldStop();
        oprot.WriteStructEnd();
      }
      finally
      {
        oprot.DecrementRecursionDepth();
      }
    }

    public override string ToString() {
      StringBuilder __sb = new StringBuilder("IndexExpression(");
      __sb.Append(", Column_name: ");
      __sb.Append(Column_name);
      __sb.Append(", Op: ");
      __sb.Append(Op);
      __sb.Append(", Value: ");
      __sb.Append(Value);
      __sb.Append(")");
      return __sb.ToString();
    }

  }

}
