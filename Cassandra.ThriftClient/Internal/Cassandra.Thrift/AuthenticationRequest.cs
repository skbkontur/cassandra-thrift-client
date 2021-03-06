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
  /// Authentication requests can contain any data, dependent on the IAuthenticator used
  /// </summary>
  #if !SILVERLIGHT
  [Serializable]
  #endif
  internal partial class AuthenticationRequest : TBase
  {

    public Dictionary<string, string> Credentials { get; set; }

    public AuthenticationRequest() {
    }

    public AuthenticationRequest(Dictionary<string, string> credentials) : this() {
      this.Credentials = credentials;
    }

    public void Read (TProtocol iprot)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_credentials = false;
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
              if (field.Type == TType.Map) {
                {
                  Credentials = new Dictionary<string, string>();
                  TMap _map40 = iprot.ReadMapBegin();
                  for( int _i41 = 0; _i41 < _map40.Count; ++_i41)
                  {
                    string _key42;
                    string _val43;
                    _key42 = iprot.ReadString();
                    _val43 = iprot.ReadString();
                    Credentials[_key42] = _val43;
                  }
                  iprot.ReadMapEnd();
                }
                isset_credentials = true;
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
        if (!isset_credentials)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Credentials not set");
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
        TStruct struc = new TStruct("AuthenticationRequest");
        oprot.WriteStructBegin(struc);
        TField field = new TField();
        if (Credentials == null)
          throw new TProtocolException(TProtocolException.INVALID_DATA, "required field Credentials not set");
        field.Name = "credentials";
        field.Type = TType.Map;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        {
          oprot.WriteMapBegin(new TMap(TType.String, TType.String, Credentials.Count));
          foreach (string _iter44 in Credentials.Keys)
          {
            oprot.WriteString(_iter44);
            oprot.WriteString(Credentials[_iter44]);
          }
          oprot.WriteMapEnd();
        }
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
      StringBuilder __sb = new StringBuilder("AuthenticationRequest(");
      __sb.Append(", Credentials: ");
      __sb.Append(Credentials);
      __sb.Append(")");
      return __sb.ToString();
    }

  }

}
