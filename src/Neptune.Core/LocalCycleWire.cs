using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

internal static class LocalCycleWire
{
    internal const int MetadataLimit = 40 * 1024;
    internal static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, MaxDepth = 32 };
    // Both receipts have already passed the complete original/status validator.
    // Re-encoding by System.Text.Json may change escapes or property order;
    // compare every value while still rejecting duplicate keys/malformed JSON.
    internal static bool SameResult(string? left, string? right)
    {
        if (left is null || right is null) return left is null && right is null;
        using var a = SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(left));
        using var b = SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(right));
        return JsonElement.DeepEquals(a.RootElement, b.RootElement);
    }
    internal static long Decimal(string value) => value.Length is >= 1 and <= 19
        && (value.Length == 1 || value[0] != '0') && value.All(c => c is >= '0' and <= '9')
        && long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var n)
        ? n : throw new InvalidDataException("Invalid exact cycle decimal.");
    internal static bool Uuid(string value) => Guid.TryParseExact(value, "D", out var id)
        && id != Guid.Empty && value == value.ToLowerInvariant();
    internal static bool Hash(string value) => value.Length == 64 && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
    internal static void Configuration(WindowsCycleConfiguration c)
    {
        using var binding=JsonDocument.Parse(BindingBytes(c.Binding));
        _ = SyncProtocolReader.Binding(binding.RootElement);
        if (!Uuid(c.Binding.RootId) || !Uuid(c.Binding.ServerEpoch) || !Uuid(c.Binding.BindingVersion)||c.RemoteAnchorId is not null&&!Uuid(c.RemoteAnchorId)
            || !Uuid(c.DeviceId) || !c.ServerOrigin.IsAbsoluteUri || c.ServerOrigin.Scheme is not ("http" or "https")
            || !string.IsNullOrEmpty(c.ServerOrigin.UserInfo) || !string.IsNullOrEmpty(c.ServerOrigin.Query)
            || !string.IsNullOrEmpty(c.ServerOrigin.Fragment) || c.ServerOrigin.AbsolutePath != "/"
            || new[] { c.MappingId, c.ClientId, c.ProfileId, c.RulesVersion, c.CredentialScope, c.BackendIdentity }.Any(s => s.Length is < 1 or > 128||s.Any(char.IsControl))
            || c.BackendIdentity.Any(ch=>!(char.IsAsciiLetterOrDigit(ch)||ch is '.' or '_' or ':' or '-')))
            throw new InvalidDataException("Cycle requires explicit enrolled local and remote identities.");
    }
    internal static object BindingObject(SyncRootBinding b) => new { protocol=b.Protocol, descriptor_schema=b.DescriptorSchema,
        server_identity=b.ServerIdentity, root_id=b.RootId, server_epoch=b.ServerEpoch, policy_version=b.PolicyVersion, binding_version=b.BindingVersion };
    internal static byte[] BindingBytes(SyncRootBinding b) => JsonSerializer.SerializeToUtf8Bytes(BindingObject(b));
    internal static object InputObject(CycleUploadInput input)
    {
        var i=input.Intent;
        object condition=i.Condition.Kind=="absent"?new {kind="absent"}:new {kind="match",nodeId=i.Condition.NodeId,metadataVersion=i.Condition.MetadataVersion};
        return new { epoch=input.Epoch,bindingVersion=input.BindingVersion,backendIdentity=input.BackendIdentity,
            intent=new { schema=i.Schema,deviceKind=i.DeviceKind,deviceId=i.DeviceId,rootId=i.RootId,requestPath=i.RequestPath,
                idempotencyKey=i.IdempotencyKey,parentId=i.ParentId,name=i.Name,targetId=i.TargetId,sizeBytes=i.SizeBytes,sha256=i.Sha256,
                ifMatch=i.IfMatch,ifNoneMatch=i.IfNoneMatch,requireAbsent=i.RequireAbsent,expectedVersionId=i.ExpectedVersionId,
                condition,parentTopology=i.ParentTopology.Select(p=>new {nodeId=p.NodeId,parentId=p.ParentId,name=p.Name}).ToArray() } };
    }
    internal static string IntentHash(CycleUploadInput input)
    {
        var i=input.Intent;_ = Decimal(i.SizeBytes);
        if(i.Schema!="saturn.upload-intent.v2"||i.DeviceKind!="windows_sync"||!Hash(i.Sha256)
            || !Uuid(i.RootId)||!Uuid(i.DeviceId)||!Uuid(i.ParentId)||!Uuid(input.Epoch)||!Uuid(input.BindingVersion)
            || i.IdempotencyKey.Length is <8 or >200||i.RequestPath.Length is <1 or >4096||i.ParentTopology.Count is <1 or >129)
            throw new InvalidDataException("Invalid sealed catalog input.");
        Name(i.Name);
        if(i.Condition.Kind=="absent")
        {if(i.TargetId is not null||i.ExpectedVersionId is not null||!i.RequireAbsent||i.IfNoneMatch!="*"||i.IfMatch is not null||i.Condition.NodeId is not null||i.Condition.MetadataVersion is not null)throw new InvalidDataException("Inconsistent create-only condition.");}
        else if(i.Condition.Kind!="match"||i.TargetId!=i.Condition.NodeId||i.TargetId is null||!Uuid(i.TargetId)
            ||i.Condition.MetadataVersion is null||!Uuid(i.Condition.MetadataVersion)||i.ExpectedVersionId is null||!Uuid(i.ExpectedVersionId)||i.RequireAbsent||i.IfNoneMatch is not null)
            throw new InvalidDataException("Invalid metadata CAS condition.");
        var seen=new HashSet<string>(StringComparer.Ordinal);
        for(var n=0;n<i.ParentTopology.Count;n++)
        {var p=i.ParentTopology[n];Name(p.Name);if(!Uuid(p.NodeId)||!seen.Add(p.NodeId)||p.ParentId is not null&&!Uuid(p.ParentId)
            ||n==0&&p.NodeId!=i.ParentId||n+1<i.ParentTopology.Count&&p.ParentId!=i.ParentTopology[n+1].NodeId
            ||n+1==i.ParentTopology.Count&&(p.NodeId!=i.RootId||p.ParentId is not null))throw new InvalidDataException("Invalid sealed topology.");}
        // Exact JSON.stringify ordered array; default .NET escaping is not its
        // byte contract for Unicode names, '<' or supplementary scalars.
        var fields=new List<object?>{i.Schema,i.DeviceKind,i.IdempotencyKey,i.DeviceId,i.RootId,i.RequestPath,i.ParentId,i.Name,
            i.TargetId,i.SizeBytes,i.Sha256,i.IfMatch,i.IfNoneMatch,i.RequireAbsent,i.ExpectedVersionId,i.Condition.Kind};
        if(i.Condition.Kind=="match"){fields.Add(i.Condition.NodeId);fields.Add(i.Condition.MetadataVersion);}
        fields.Add(i.ParentTopology.Select(p=>new object?[]{p.NodeId,p.ParentId,p.Name}).ToArray());
        var text=new StringBuilder();Ecma(text,fields);return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
    }
    private static void Ecma(StringBuilder b,object? value)
    {
        if(value is null){b.Append("null");return;}if(value is bool flag){b.Append(flag?"true":"false");return;}
        if(value is string s){b.Append('"');foreach(var c in s){if(c=='"'||c=='\\')b.Append('\\').Append(c);else if(c<32)b.Append(c switch{'\b'=>"\\b",'\f'=>"\\f",'\n'=>"\\n",'\r'=>"\\r",'\t'=>"\\t",_=>"\\u"+((int)c).ToString("x4",CultureInfo.InvariantCulture)});else b.Append(c);}b.Append('"');return;}
        if(value is System.Collections.IEnumerable items){b.Append('[');var comma=false;foreach(var item in items){if(comma)b.Append(',');Ecma(b,item);comma=true;}b.Append(']');return;}
        throw new InvalidDataException("Unexpected fingerprint value.");
    }
    internal static void Name(string value)
    {
        if(value.Length==0||value is "." or ".."||value.IndexOfAny(['\0','/','\\'])>=0)throw new InvalidDataException("Invalid exact catalog name.");
        var count=0;for(var n=0;n<value.Length;n++){var c=value[n];if(char.IsControl(c))throw new InvalidDataException("Unsupported name control.");if(char.IsHighSurrogate(c)){if(++n>=value.Length||!char.IsLowSurrogate(value[n]))throw new InvalidDataException("Invalid name scalar.");}else if(char.IsLowSurrogate(c))throw new InvalidDataException("Invalid name scalar.");count++;}
        if(count>255)throw new InvalidDataException("Catalog name exceeds scalar limit.");
    }
    private static JsonElement Field(JsonElement e,string key)=>e.TryGetProperty(key,out var v)?v:throw new InvalidDataException("Catalog field is missing.");
    private static string Text(JsonElement e,string key)=>Field(e,key).ValueKind==JsonValueKind.String?Field(e,key).GetString()!:throw new InvalidDataException("Catalog field must be a string.");
    private static bool Flag(JsonElement e,string key)=>Field(e,key).ValueKind is JsonValueKind.True or JsonValueKind.False?Field(e,key).GetBoolean():throw new InvalidDataException("Catalog flag must be a boolean.");
    private static void Fields(JsonElement e,params string[] keys)
    {if(e.ValueKind!=JsonValueKind.Object||e.EnumerateObject().Count()!=keys.Length||e.EnumerateObject().Any(p=>!keys.Contains(p.Name,StringComparer.Ordinal)))throw new InvalidDataException("Unexpected catalog fields.");}
    internal static CycleUploadStatus Status(ReadOnlyMemory<byte> bytes,CycleUploadInput input,string hash,string? upload)
    {
        if(bytes.Length>MetadataLimit)throw new InvalidDataException("Catalog status exceeds limit.");using var json=SyncProtocolReader.Parse(bytes);var top=json.RootElement;
        Fields(top,top.TryGetProperty("result",out var result)?["session","result","replayed"]:["session","replayed"]);
        var s=Field(top,"session");Fields(s,"uploadId","operationId","rootId","intentHash","expectedSizeBytes","receivedSizeBytes","phase","transferPending");
        var id=Text(s,"uploadId");var phase=Text(s,"phase");var expected=Decimal(Text(s,"expectedSizeBytes"));var received=Decimal(Text(s,"receivedSizeBytes"));
        if(!Uuid(id)||Text(s,"operationId")!=id||upload is not null&&id!=upload||Text(s,"rootId")!=input.Intent.RootId
            ||Text(s,"intentHash")!=hash||hash!=IntentHash(input)||expected!=Decimal(input.Intent.SizeBytes)||received>expected
            ||phase is not ("transport" or "verified" or "physical_pending" or "reconciliation_required" or "published" or "not_published"))
            throw new InvalidDataException("Catalog status cannot rebind its original operation or bytes.");
        SyncDescriptor? descriptor=null;long? first=null,last=null;string? rawResult=null;
        if(result.ValueKind!=JsonValueKind.Undefined)
        {
            var published=Text(result,"status")=="PUBLISHED";
            Fields(result,published?["schema","status","uploadId","operationId","rootId","intentHash","publicationReceipt","publishedDescription","firstSeq","lastSeq"]:["schema","status","uploadId","operationId","rootId","intentHash","reason"]);
            if(Text(result,"schema")!="saturn.catalog-upload-result.v2"||Text(result,"uploadId")!=id||Text(result,"operationId")!=id||Text(result,"rootId")!=input.Intent.RootId||Text(result,"intentHash")!=hash)
                throw new InvalidDataException("Historical result identity changed.");
            if(published)
            {
                if(phase!="published"||received!=expected||Flag(s,"transferPending"))throw new InvalidDataException("Published status lacks complete transport.");
                var receipt=Field(result,"publicationReceipt");Fields(receipt,"schema","uploadId","operationId","intentHash","rootId","resourceId","versionId","parentId","name","sizeBytes","sha256","etag","publishedAt");
                var i=input.Intent;
                if(Text(receipt,"schema")!="saturn.upload-publication.v1"||Text(receipt,"uploadId")!=id||Text(receipt,"operationId")!=id
                    ||Text(receipt,"intentHash")!=hash||Text(receipt,"rootId")!=i.RootId||Text(receipt,"parentId")!=i.ParentId||Text(receipt,"name")!=i.Name
                    ||Text(receipt,"sizeBytes")!=i.SizeBytes||Text(receipt,"sha256")!=i.Sha256||Text(receipt,"etag")!="\"sha256-"+i.Sha256+"\""
                    ||!Uuid(Text(receipt,"resourceId"))||!Uuid(Text(receipt,"versionId"))
                    ||!DateTimeOffset.TryParseExact(Text(receipt,"publishedAt"),"yyyy-MM-dd'T'HH:mm:ss.fff'Z'",CultureInfo.InvariantCulture,DateTimeStyles.AssumeUniversal,out _))
                    throw new InvalidDataException("Immutable publication receipt binding is invalid.");
                DescriptorFields(Field(result,"publishedDescription"));descriptor=SyncProtocolReader.Descriptor(Field(result,"publishedDescription"));
                if(descriptor.Type!="file"||descriptor.Availability!="AVAILABLE"||descriptor.NodeId!=Text(receipt,"resourceId")||descriptor.ParentId!=i.ParentId||descriptor.Name!=i.Name
                    ||descriptor.SizeBytes!=i.SizeBytes||descriptor.ContentSha256!=i.Sha256||descriptor.ContentVersionId!=Text(receipt,"versionId")||i.TargetId is not null&&descriptor.NodeId!=i.TargetId)
                    throw new InvalidDataException("Historical descriptor differs from sealed publication.");
                first=Decimal(Text(result,"firstSeq"));last=Decimal(Text(result,"lastSeq"));if(last<first||first==0)throw new InvalidDataException("Invalid historical sequence range.");
            }
            else if(Text(result,"status")!="NOT_PUBLISHED"||Text(result,"reason")!="cancelled_before_publication"||phase!="not_published")throw new InvalidDataException("Invalid non-publication result.");
            rawResult=result.GetRawText();
        }
        else if(phase is "published" or "not_published")throw new InvalidDataException("Terminal immutable result is missing.");
        return new(id,id,input.Intent.RootId,hash,expected,received,phase,Flag(s,"transferPending"),Flag(top,"replayed"),rawResult,descriptor,first,last);
    }
    internal static object FolderObject(CycleFolderInput i)=>new{operationId=i.OperationId,idempotencyKey=i.IdempotencyKey,rootId=i.RootId,epoch=i.Epoch,bindingVersion=i.BindingVersion,parentId=i.ParentId,name=i.Name,mode=i.Mode,parentTopology=i.ParentTopology.Select(p=>new{nodeId=p.NodeId,parentId=p.ParentId,name=p.Name}).ToArray()};
    internal static void FolderInput(CycleFolderInput i)
    {
        // Windows mapping anchors persist their original bare operation UUID
        // as the key. Preserve that immutable intent on replay; only a direct
        // root ensure has this legacy shape. Cycle jobs keep their prefix.
        var mappingAnchor=i.IdempotencyKey==i.OperationId&&i.ParentId==i.RootId&&i.Mode=="ensure";
        if(!Uuid(i.OperationId)||!Uuid(i.RootId)||!Uuid(i.Epoch)||!Uuid(i.BindingVersion)||!Uuid(i.ParentId)||i.Mode is not("ensure" or "create")||!mappingAnchor&&i.IdempotencyKey!="neptune-cycle-"+i.OperationId||i.ParentTopology.Count is <1 or >129)throw new InvalidDataException("Invalid original folder v2 input.");Name(i.Name);var seen=new HashSet<string>();
        for(var n=0;n<i.ParentTopology.Count;n++){var p=i.ParentTopology[n];Name(p.Name);if(!Uuid(p.NodeId)||!seen.Add(p.NodeId)||p.ParentId is not null&&!Uuid(p.ParentId)||n==0&&p.NodeId!=i.ParentId||n+1<i.ParentTopology.Count&&p.ParentId!=i.ParentTopology[n+1].NodeId||n+1==i.ParentTopology.Count&&(p.NodeId!=i.RootId||p.ParentId is not null))throw new InvalidDataException("Folder topology rebind/cycle.");}
    }
    internal static CycleFolderOutcome Folder(ReadOnlyMemory<byte> raw,CycleFolderInput original)
    {
        if(raw.Length>MetadataLimit)throw new InvalidDataException("Folder result exceeds limit.");using var json=SyncProtocolReader.Parse(raw);var top=json.RootElement;Fields(top,"replayed","result");_ = Flag(top,"replayed");var r=Field(top,"result");
        Fields(r,"schema","operationId","intentHash","rootId","status","description");
        FolderInput(original);if(Text(r,"schema")!="saturn.catalog-folder-result.v2"||Text(r,"operationId")!=original.OperationId||Text(r,"rootId")!=original.RootId||!Hash(Text(r,"intentHash"))||Text(r,"status") is not ("CREATED" or "EXISTING"))throw new InvalidDataException("Folder operation cannot rebind.");
        DescriptorFields(Field(r,"description"));var d=SyncProtocolReader.Descriptor(Field(r,"description"));if(d.Type!="folder"||d.ParentId!=original.ParentId||d.Name!=original.Name)throw new InvalidDataException("Folder result raw target/type changed.");return new(d,top.GetRawText(),Text(r,"intentHash"));
    }
    internal static void DescriptorFields(JsonElement e)=>Fields(e,"node_id","parent_id","name","type","size_bytes","content_sha256","content_version_id","version","availability");
}
