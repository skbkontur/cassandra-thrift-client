namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ValidationResult
    {
        private ValidationResult(string message, ValidationStatus status)
        {
            Message = message;
            Status = status;
        }

        public static ValidationResult Ok()
        {
            return new ValidationResult("", ValidationStatus.Ok);
        }

        public static ValidationResult Error(string message)
        {
            return new ValidationResult(message, ValidationStatus.Error);
        }

        public string Message { get; private set; }
        public ValidationStatus Status { get; private set; }
    }
}